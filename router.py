from __future__ import annotations

from datetime import date
from typing import Optional, Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text
from starlette.responses import StreamingResponse

from app.core.deps import get_db, get_current_user
from app.models.user import User

from .schemas import AdminAttendanceSettings, AttendanceReportResponse, AttendanceDetailsResponse, DaySessionsResponse, DayCorrectionRequest, DayCorrectionResponse
from .service import fetch_summary_report, get_settings, save_settings, fetch_details, build_excel, get_day_sessions, apply_day_correction
from .auto_close import preview_auto_close, run_auto_close
from .scheduler import start_scheduler, get_status, reload_schedule

router = APIRouter(prefix="/attendance", tags=["Admin"])


def _is_admin_db(db: Session, user: User) -> bool:
    role_id = getattr(user, "role_id", None)
    if not role_id:
        return False
    code = db.execute(text("SELECT code FROM roles WHERE id = :id"), {"id": role_id}).scalar()
    return code == "ADMIN"


def _require_admin(db: Session, user: User) -> None:
    if not _is_admin_db(db, user):
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다.")


@router.get("/settings", response_model=AdminAttendanceSettings)
def read_settings(db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    _require_admin(db, current_user)
    return get_settings(db)


@router.put("/settings", response_model=AdminAttendanceSettings)
def update_settings(payload: AdminAttendanceSettings, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    _require_admin(db, current_user)
    saved = save_settings(db, payload)
    try:
        reload_schedule()
    except Exception:
        import logging
        logging.getLogger("admin_attendance.scheduler").exception("failed to reload scheduler")
    return saved


@router.get("/report", response_model=AttendanceReportResponse)
def report(
    period: Literal["day", "month", "year"] = Query(default="month"),
    start_date: date = Query(...),
    end_date: date = Query(...),
    user_id: Optional[int] = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(db, current_user)
    items, settings = fetch_summary_report(db, start_date=start_date, end_date=end_date, user_id=user_id)
    return AttendanceReportResponse(period=period, start_date=start_date, end_date=end_date, settings=settings, items=items)


@router.get("/details", response_model=AttendanceDetailsResponse)
def details(
    start_date: date = Query(...),
    end_date: date = Query(...),
    user_id: int = Query(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(db, current_user)
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
    """특정 직원/특정 날짜(work_date_basis) 세션 원장 조회"""
    _require_admin(db, current_user)
    return get_day_sessions(db, user_id=user_id, work_date=work_date)


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
            editor_user_id=getattr(current_user, "id", 0) or 0,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return DayCorrectionResponse(ok=True)


@router.get("/excel")
def excel(
    period: Literal["day", "month", "year"] = Query(default="month"),
    start_date: date = Query(...),
    end_date: date = Query(...),
    user_id: Optional[int] = Query(default=None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    _require_admin(db, current_user)
    content = build_excel(db, start_date=start_date, end_date=end_date, user_id=user_id)

    filename = f"attendance_{period}_{start_date.isoformat()}_{end_date.isoformat()}.xlsx"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(
        iter([content]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@router.get("/auto-close/status")
def auto_close_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """자동퇴근 스케줄러 상태"""
    _require_admin(db, current_user)
    # 상태 조회 자체는 DB를 건드릴 필요 없지만, 권한체크용으로 db/current_user를 받음
    return get_status()


@router.get("/auto-close/preview")
def auto_close_preview(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """자동퇴근 확정 대상 미리보기(건수)"""
    _require_admin(db, current_user)
    return preview_auto_close(db)


@router.post("/auto-close/run")
def auto_close_run(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user),
):
    """자동퇴근 확정 실행(end_at 업데이트)"""
    _require_admin(db, current_user)
    return run_auto_close(db)


# ✅ 완전자동화: 라우터 로드 시 스케줄러 자동 시작
try:
    start_scheduler()
except Exception:
    # 서버 기동 자체를 막지 않도록 로깅만
    import logging
    logging.getLogger("admin_attendance.scheduler").exception("failed to start scheduler on import")
