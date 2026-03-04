from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import text

from app.core.deps import get_db
from .auto_close import run_auto_close
from .service import get_settings

log = logging.getLogger("admin_attendance.scheduler")

_KST = ZoneInfo("Asia/Seoul")
_scheduler: BackgroundScheduler | None = None
_last_run_time: str | None = None
_last_run_ok: bool | None = None

# 멀티 워커 중복 실행 방지용(고정 키)
_LOCK_KEY = 928374  # 프로젝트 내에서만 유일하면 됨
_SCHED_LOCK_KEY = _LOCK_KEY + 1  # 스케줄러 단일 실행용(프로세스 간)

# 스케줄러 단일 실행을 위해 DB advisory lock을 '프로세스 생존 동안' 유지
_sched_db_gen = None
_sched_db = None
_sched_lock_acquired = False


def _parse_hms(value):
    """입력 값이 str 또는 datetime.time일 수 있음.
    - 'HH:MM:SS' / 'HH:MM' 문자열
    - datetime.time 객체
    """
    if value is None:
        return None
    # datetime.time 지원
    try:
        import datetime as _dt
        if isinstance(value, _dt.time):
            return value.hour, value.minute
    except Exception:
        pass
    # 문자열 지원
    if not isinstance(value, str):
        return None
    v = value.strip()
    if len(v) >= 5:
        try:
            hh = int(v[0:2])
            mm = int(v[3:5])
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return hh, mm
        except Exception:
            return None
    return None


def _calc_trigger_time_from_settings():
    """설정 우선순위:
    1) auto_close_run_time (대표님이 직접 지정)
    2) day/night cutoff 중 가장 늦은 시각 + 5분
    3) 기본 08:05
    """
    db_gen = get_db()
    db = next(db_gen)
    try:
        s = get_settings(db)

        rt = _parse_hms(getattr(s, "auto_close_run_time", None))
        if rt is not None:
            return rt[0], rt[1]

        c1 = _parse_hms(getattr(s, "day_auto_checkout_cutoff", None))
        c2 = _parse_hms(getattr(s, "night_auto_checkout_cutoff", None))
        candidates = [x for x in [c1, c2] if x is not None]
        if not candidates:
            return 8, 5

        hh, mm = max(candidates, key=lambda x: x[0] * 60 + x[1])
        total = (hh * 60 + mm + 5) % (24 * 60)
        return total // 60, total % 60
    except Exception:
        log.exception("failed to calc trigger time; fallback 08:05")
        return 8, 5
    finally:
        try:
            next(db_gen)
        except StopIteration:
            pass


def _try_pg_lock(db, key: int = _LOCK_KEY) -> bool:
    """PostgreSQL advisory lock

    Args:
        db: SQLAlchemy session
        key: advisory lock key
    """
    try:
        got = db.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": key}).scalar()
        return bool(got)
    except Exception:
        # PG가 아니거나 권한 문제면 lock없이 진행(개발환경 대응)
        return True


def _release_pg_lock(db, key: int = _LOCK_KEY) -> None:
    try:
        db.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": key})
    except Exception:
        pass



def _ensure_scheduler_singleton() -> bool:
    """프로세스 간 스케줄러 단일 실행을 보장한다.

    - PostgreSQL advisory lock은 **세션/커넥션 단위**로 유지된다.
    - 따라서 lock을 잡은 DB 세션을 모듈 전역으로 보관하여, 프로세스 생존 동안 lock을 유지한다.
    """
    global _sched_db_gen, _sched_db, _sched_lock_acquired

    if _sched_lock_acquired:
        return True

    try:
        _sched_db_gen = get_db()
        _sched_db = next(_sched_db_gen)

        _sched_lock_acquired = _try_pg_lock(_sched_db, _SCHED_LOCK_KEY)
        if not _sched_lock_acquired:
            # 다른 프로세스가 이미 스케줄러를 띄움
            try:
                next(_sched_db_gen)
            except StopIteration:
                pass
            _sched_db_gen = None
            _sched_db = None
            return False

        log.info("scheduler singleton lock acquired (key=%s)", _SCHED_LOCK_KEY)
        return True
    except Exception:
        log.exception("failed to acquire scheduler singleton lock")
        _sched_lock_acquired = False
        # best-effort cleanup
        try:
            if _sched_db_gen:
                next(_sched_db_gen)
        except Exception:
            pass
        _sched_db_gen = None
        _sched_db = None
        return False


def _release_scheduler_singleton_lock() -> None:
    global _sched_db_gen, _sched_db, _sched_lock_acquired
    if not _sched_lock_acquired or not _sched_db:
        return
    try:
        _unlock_pg_lock(_sched_db, _SCHED_LOCK_KEY)
        log.info("scheduler singleton lock released (key=%s)", _SCHED_LOCK_KEY)
    except Exception:
        pass
    finally:
        _sched_lock_acquired = False
        try:
            if _sched_db_gen:
                next(_sched_db_gen)
        except Exception:
            pass
        _sched_db_gen = None
        _sched_db = None
def _job():
    """매일 자동퇴근 확정 실행"""
    global _last_run_time, _last_run_ok

    db_gen = get_db()
    db = next(db_gen)
    locked = False
    try:
        locked = _try_pg_lock(db)
        if not locked:
            log.info("auto-close skipped (lock not acquired)")
            return

        result = run_auto_close(db)
        _last_run_time = datetime.now(_KST).strftime("%Y-%m-%d %H:%M:%S")
        _last_run_ok = True
        log.info("auto-close done: %s", result)
    except Exception:
        _last_run_time = datetime.now(_KST).strftime("%Y-%m-%d %H:%M:%S")
        _last_run_ok = False
        log.exception("auto-close failed")
        try:
            db.rollback()
        except Exception:
            pass
    finally:
        if locked:
            _release_pg_lock(db)
        try:
            next(db_gen)
        except StopIteration:
            pass


def start_scheduler():
    """중복 실행 방지 포함(프로세스 내) + 프로세스 간 단일 실행 보장(DB lock)"""
    global _scheduler
    if _scheduler and _scheduler.running:
        return

    # ✅ 프로세스 간 단일 실행: lock을 못 잡으면 이 프로세스에서는 스케줄러를 띄우지 않음
    if not _ensure_scheduler_singleton():
        log.info("scheduler singleton lock not acquired; skip starting scheduler in this process")
        return


    hour, minute = _calc_trigger_time_from_settings()

    _scheduler = BackgroundScheduler(timezone=_KST)
    trigger = CronTrigger(hour=hour, minute=minute, second=0, timezone=_KST)
    _scheduler.add_job(
        _job,
        trigger,
        id="admin_attendance_auto_close",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )
    _scheduler.start()
    log.info("admin attendance scheduler started (KST %02d:%02d)", hour, minute)


def reload_schedule():
    """설정 변경 후 스케줄 실행 시각을 즉시 반영"""
    global _scheduler
    if not _scheduler:
        start_scheduler()
        return

    hour, minute = _calc_trigger_time_from_settings()
    trigger = CronTrigger(hour=hour, minute=minute, second=0, timezone=_KST)
    _scheduler.add_job(
        _job,
        trigger,
        id="admin_attendance_auto_close",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )
    log.info("admin attendance scheduler reloaded (KST %02d:%02d)", hour, minute)


def get_status():
    """프론트 표시용 상태"""
    if not _scheduler:
        return {
            "running": False,
            "next_run_time": None,
            "last_run_time": _last_run_time,
            "last_run_ok": _last_run_ok,
        }

    try:
        job = _scheduler.get_job("admin_attendance_auto_close")
        nrt = (
            job.next_run_time.astimezone(_KST).strftime("%Y-%m-%d %H:%M:%S")
            if job and job.next_run_time
            else None
        )
    except Exception:
        nrt = None

    return {
        "running": bool(_scheduler.running),
        "next_run_time": nrt,
        "last_run_time": _last_run_time,
        "last_run_ok": _last_run_ok,
    }