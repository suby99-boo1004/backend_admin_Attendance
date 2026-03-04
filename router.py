from __future__ import annotations

from datetime import date
from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.orm import Session
from starlette.responses import StreamingResponse

from app.core.deps import get_db, get_current_user
from app.models.user import User

from .auto_close import preview_auto_close, run_auto_close
from .scheduler import get_status, reload_schedule, start_scheduler
from .schemas import (
    AdminAttendanceSettings,
    AttendanceDetailsResponse,
    AttendanceReportResponse,
    DayCorrectionRequest,
    DayCorrectionResponse,
    DaySessionsResponse,
)
from .service import (
    apply_day_correction,
    build_excel,
    fetch_details,
    fetch_summary_report,
    get_day_sessions,
    get_settings,
    save_settings,
)

# ✅ /api/admin 아래에 include_router 되는 전제
router = APIRouter(prefix="/attendance", tags=["admin_attendance"])

# =========================================================
# 권한 정책(대표님 최종)
# - 조회/검색/집계/엑셀(READ): 내부직원(관리자/운영자/회사직원) 누구나 가능
# - 설정/정정/자동확정 실행(WRITE): 관리자만 가능
# 내부직원 정의(혼용 대비):
#   - role_id in (6,7,8) OR roles.code in ('ADMIN','OPERATOR','STAFF')
# =========================================================

_ALLOWED_INTERNAL_ROLE_IDS = (6, 7, 8)
_ALLOWED_INTERNAL_ROLE_CODES = ("ADMIN", "OPERATOR", "STAFF")


def _get_role_code(db: Session, user: User) -> Optional[str]:
    role_id = getattr(user, "role_id", None)
    if not role_id:
        return None
    try:
        return db.execute(text("SELECT code FROM roles WHERE id = :id"), {"id": int(role_id)}).scalar()
    except Exception:
        return None


def _is_internal(db: Session, user: User) -> bool:
    # 1) role_id 우선
    rid = getattr(user, "role_id", None)
    try:
        rid_i = int(rid) if rid is not None else None
    except Exception:
        rid_i = None
    if rid_i in _ALLOWED_INTERNAL_ROLE_IDS:
        return True

    # 2) user.role_code가 있으면 사용
    role_code = getattr(user, "role_code", None)
    if isinstance(role_code, str) and role_code.upper() in _ALLOWED_INTERNAL_ROLE_CODES:
        return True

    # 3) roles 테이블 조회
    code = _get_role_code(db, user)
    if isinstance(code, str) and code.upper() in _ALLOWED_INTERNAL_ROLE_CODES:
        return True

    return False


def _require_internal(db: Session, user: User) -> None:
    if not _is_internal(db, user):
        raise HTTPException(status_code=403, detail="내부 직원만 접근 가능합니다.")


def _is_admin(db: Session, user: User) -> bool:
    code = _get_role_code(db, user)
    return (code or "").upper() == "ADMIN"


def _require_admin(db: Session, user: User) -> None:
    if not _is_admin(db, user):
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다.")


# -------------------------
# WRITE: 관리자만
# -------------------------
@router.get("/settings", response_model=AdminAttendanceSettings)
def read_settings(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(db, current_user)
    return get_settings(db)


@router.put("/settings", response_model=AdminAttendanceSettings)
def update_settings(
    payload: AdminAttendanceSettings,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(db, current_user)
    saved = save_settings(db, payload)
    # 스케줄 즉시 반영(실패해도 저장 자체는 성공)
    try:
        reload_schedule()
    except Exception:
        import logging

        logging.getLogger("admin_attendance.scheduler").exception("failed to reload scheduler")
    return saved


@router.post("/day/correct", response_model=DayCorrectionResponse)
def day_correct(
    payload: DayCorrectionRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """관리자 근태 정정(출근/퇴근 날짜+시간)"""
    _require_admin(db, current_user)
    try:
        apply_day_correction(
            db,
            user_id=payload.user_id,
            work_date=payload.work_date,
            start_date=payload.start_date,
            start_hm=payload.start_hm,
            end_date=payload.end_date,
            end_hm=payload.end_hm,
            reason=payload.reason,
            editor_user_id=int(getattr(current_user, "id", 0) or 0),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return DayCorrectionResponse(ok=True)


@router.get("/auto-close/status")
def auto_close_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(db, current_user)
    return get_status()


@router.get("/auto-close/preview")
def auto_close_preview(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(db, current_user)
    return preview_auto_close(db)


@router.post("/auto-close/run")
def auto_close_run(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(db, current_user)
    return run_auto_close(db)


# -------------------------
# READ: 내부직원 누구나
# -------------------------
@router.get("/report", response_model=AttendanceReportResponse)
def report(
    period: Literal["day", "month", "year"] = Query(default="month"),
    start_date: date = Query(...),
    end_date: date = Query(...),
    user_id: Optional[int] = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_internal(db, current_user)
    items, settings = fetch_summary_report(db, start_date=start_date, end_date=end_date, user_id=user_id)
    return AttendanceReportResponse(
        period=period,
        start_date=start_date,
        end_date=end_date,
        settings=settings,
        items=items,
    )


@router.get("/details", response_model=AttendanceDetailsResponse)
def details(
    start_date: date = Query(...),
    end_date: date = Query(...),
    user_id: int = Query(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    # ✅ 직원도 조회 가능 (내부직원)
    _require_internal(db, current_user)
    try:
        return fetch_details(db, start_date=start_date, end_date=end_date, user_id=user_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/day/sessions", response_model=DaySessionsResponse)
def day_sessions(
    user_id: int = Query(...),
    work_date: date = Query(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_internal(db, current_user)
    return get_day_sessions(db, user_id=user_id, work_date=work_date)


@router.get("/excel")
def excel(
    period: Literal["day", "month", "year"] = Query(default="month"),
    start_date: date = Query(...),
    end_date: date = Query(...),
    user_id: Optional[int] = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_internal(db, current_user)
    content = build_excel(db, start_date=start_date, end_date=end_date, user_id=user_id)
    filename = f"attendance_{period}_{start_date.isoformat()}_{end_date.isoformat()}.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        iter([content]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


# ✅ 스케줄러 자동 시작(기존 유지)
try:
    start_scheduler()
except Exception:
    import logging

    logging.getLogger("admin_attendance.scheduler").exception("failed to start scheduler on import")
