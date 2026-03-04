from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import text
from sqlalchemy.orm import Session

from .service import get_settings, _get_table_columns  # 기존 service 유틸/설정 재사용

_KST = ZoneInfo("Asia/Seoul")


def preview_auto_close(db: Session) -> dict:
    """자동퇴근 확정 대상 카운트 미리보기"""
    settings = get_settings(db)

    day_cut_h, day_cut_m = settings.day_auto_checkout_cutoff.hour, settings.day_auto_checkout_cutoff.minute
    night_cut_h, night_cut_m = settings.night_auto_checkout_cutoff.hour, settings.night_auto_checkout_cutoff.minute

    # work_date_basis 기준으로 cutoff 계산
    sql = text(f"""
        SELECT
          SUM(CASE WHEN end_at IS NULL AND shift_type='DAY'
            AND now() >= make_timestamptz(
              EXTRACT(YEAR FROM (work_date_basis + interval '1 day'))::int,
              EXTRACT(MONTH FROM (work_date_basis + interval '1 day'))::int,
              EXTRACT(DAY FROM (work_date_basis + interval '1 day'))::int,
              {day_cut_h}, {day_cut_m}, 0, 'Asia/Seoul')
            THEN 1 ELSE 0 END) AS day_cnt,

          SUM(CASE WHEN end_at IS NULL AND shift_type='NIGHT'
            AND now() >= make_timestamptz(
              EXTRACT(YEAR FROM (work_date_basis + interval '1 day'))::int,
              EXTRACT(MONTH FROM (work_date_basis + interval '1 day'))::int,
              EXTRACT(DAY FROM (work_date_basis + interval '1 day'))::int,
              {night_cut_h}, {night_cut_m}, 0, 'Asia/Seoul')
            THEN 1 ELSE 0 END) AS night_cnt
        FROM work_sessions
    """)
    row = db.execute(sql).mappings().first() or {}
    return {"day": int(row.get("day_cnt") or 0), "night": int(row.get("night_cnt") or 0)}


def run_auto_close(db: Session) -> dict:
    """자동퇴근 확정 실행: end_at NULL 세션을 규칙에 따라 end_at 업데이트"""
    settings = get_settings(db)

    day_end_h, day_end_m = settings.day_regular_checkout_time.hour, settings.day_regular_checkout_time.minute
    day_cut_h, day_cut_m = settings.day_auto_checkout_cutoff.hour, settings.day_auto_checkout_cutoff.minute

    night_end_h, night_end_m = settings.night_regular_checkout_time.hour, settings.night_regular_checkout_time.minute
    night_cut_h, night_cut_m = settings.night_auto_checkout_cutoff.hour, settings.night_auto_checkout_cutoff.minute

    ws_cols = _get_table_columns(db, "work_sessions")
    has_source = "source" in ws_cols
    has_updated = "updated_at" in ws_cols

    extra_set = []
    if has_source:
        extra_set.append("source = 'AUTO_CLOSE'")
    if has_updated:
        extra_set.append("updated_at = now()")
    extra_sql = (", " + ", ".join(extra_set)) if extra_set else ""

    day_sql = text(f"""
        UPDATE work_sessions
        SET end_at = make_timestamptz(
              EXTRACT(YEAR FROM work_date_basis)::int,
              EXTRACT(MONTH FROM work_date_basis)::int,
              EXTRACT(DAY FROM work_date_basis)::int,
              {day_end_h}, {day_end_m}, 0, 'Asia/Seoul'
            ){extra_sql}
        WHERE end_at IS NULL
          AND shift_type = 'DAY'
          AND now() >= make_timestamptz(
              EXTRACT(YEAR FROM (work_date_basis + interval '1 day'))::int,
              EXTRACT(MONTH FROM (work_date_basis + interval '1 day'))::int,
              EXTRACT(DAY FROM (work_date_basis + interval '1 day'))::int,
              {day_cut_h}, {day_cut_m}, 0, 'Asia/Seoul'
          )
        RETURNING id
    """)
    day_ids = [r[0] for r in db.execute(day_sql).all()]

    night_sql = text(f"""
        UPDATE work_sessions
        SET end_at = make_timestamptz(
              EXTRACT(YEAR FROM work_date_basis)::int,
              EXTRACT(MONTH FROM work_date_basis)::int,
              EXTRACT(DAY FROM work_date_basis)::int,
              {night_end_h}, {night_end_m}, 0, 'Asia/Seoul'
            ){extra_sql}
        WHERE end_at IS NULL
          AND shift_type = 'NIGHT'
          AND now() >= make_timestamptz(
              EXTRACT(YEAR FROM (work_date_basis + interval '1 day'))::int,
              EXTRACT(MONTH FROM (work_date_basis + interval '1 day'))::int,
              EXTRACT(DAY FROM (work_date_basis + interval '1 day'))::int,
              {night_cut_h}, {night_cut_m}, 0, 'Asia/Seoul'
          )
        RETURNING id
    """)
    night_ids = [r[0] for r in db.execute(night_sql).all()]

    db.commit()
    return {"day_updated": len(day_ids), "night_updated": len(night_ids)}
