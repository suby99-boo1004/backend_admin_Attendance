from __future__ import annotations

from datetime import date, time, datetime
from typing import Literal, Optional, List

from pydantic import BaseModel, Field

Period = Literal["day", "month", "year"]


class AdminAttendanceSettings(BaseModel):
    # 야근/자동퇴근
    overtime_threshold_hours: float = Field(default=12.0, ge=0, le=24)

    # ✅ 추가 업무 인정 기준(시간)
    # 예: 15시간 초과 근무한 날은 '추가 업무 1일'로 인정
    extra_work_threshold_hours: float = Field(default=15.0, ge=0, le=72)

    day_auto_checkout_cutoff: time = Field(default=time(8, 0))
    day_regular_checkout_time: time = Field(default=time(18, 30))

    night_auto_checkout_cutoff: time = Field(default=time(8, 0))
    night_regular_checkout_time: time = Field(default=time(6, 0))

    # 자동퇴근 스케줄 실행 시각(완전자동화)
    auto_close_run_time: time = Field(default=time(8, 5))

    assume_night_start_hour_from: int = Field(default=18, ge=0, le=23)
    assume_night_start_hour_to: int = Field(default=5, ge=0, le=23)

    # 코드값(대표님 실데이터)
    office_session_types: List[str] = Field(default_factory=lambda: ["OFFICE"])
    offsite_session_types: List[str] = Field(default_factory=lambda: ["OUTSIDE"])
    leave_session_types: List[str] = Field(default_factory=lambda: ["LEAVE"])
    half_leave_session_types: List[str] = Field(default_factory=list)

    day_shift_types: List[str] = Field(default_factory=lambda: ["DAY"])
    night_shift_types: List[str] = Field(default_factory=lambda: ["NIGHT"])

    # 컬럼명
    session_type_column: str = Field(default="session_type")
    shift_type_column: str = Field(default="shift_type")
    place_column: str = Field(default="place")
    task_column: str = Field(default="task")
    is_holiday_column: str = Field(default="is_holiday")


class SummaryEmployeeMetric(BaseModel):
    user_id: int
    user_name: str

    total_work_minutes: int
    total_work_hours: float
    avg_work_hours: float

    overtime_days: int
    holiday_work_days: int

    # ✅ 추가 업무 인정(일)
    extra_work_days: int = 0

    annual_leave_total: int
    annual_leave_used: int
    annual_leave_used_this_period: int

    half_leave_used: int

    offsite_count: int
    office_count: int

    offsite_places: List[str] = Field(default_factory=list)


class AttendanceReportResponse(BaseModel):
    period: Period
    start_date: date
    end_date: date
    settings: AdminAttendanceSettings
    items: List[SummaryEmployeeMetric]


class DailyDetailRow(BaseModel):
    work_date: date
    shift_type: Optional[str] = None
    is_holiday: bool = False
    work_minutes: int = 0
    work_hours: float = 0.0
    session_types: List[str] = Field(default_factory=list)
    places: List[str] = Field(default_factory=list)
    tasks: List[str] = Field(default_factory=list)

    # ✅ 운영관리-근태관리 UI에서 '날짜+시간' 표시/프리필을 위해 추가
    first_start_at: Optional[datetime] = None
    last_end_at: Optional[datetime] = None

    # 월차/반차도 날짜+시간으로 표시할 수 있도록(해당 유형 세션의 최초 start_at)
    leave_start_at: Optional[datetime] = None
    half_leave_start_at: Optional[datetime] = None

    # ✅ 추가 업무 인정 여부(일자 총 근무시간이 기준시간 초과)
    extra_work_recognized: bool = False


class AttendanceDetailsResponse(BaseModel):
    user_id: int
    user_name: str
    start_date: date
    end_date: date
    days: List[DailyDetailRow]


# =========================
# 관리자 근태 정정(수정)
# =========================

class WorkSessionRow(BaseModel):
    id: int
    session_type: Optional[str] = None
    shift_type: Optional[str] = None
    start_at: Optional[datetime] = None
    end_at: Optional[datetime] = None
    # end_at이 NULL인 경우, 설정 규칙으로 인정되는 종료시각(표시/검증용)
    effective_end_at: Optional[datetime] = None
    # start_at ~ effective_end_at(또는 end_at) 기준 분(min)
    work_minutes: Optional[int] = None
    work_date_basis: Optional[date] = None
    place: Optional[str] = None
    task: Optional[str] = None
    is_holiday: Optional[bool] = None
    source: Optional[str] = None


class DaySessionsResponse(BaseModel):
    user_id: int
    work_date: date
    sessions: List[WorkSessionRow] = Field(default_factory=list)

    # ✅ 정정(수정) 모달 프리필용
    first_start_at: Optional[datetime] = None
    last_end_at: Optional[datetime] = None


class DayCorrectionRequest(BaseModel):
    user_id: int
    work_date: date  # 선택한 날짜(row 기준, work_date_basis)
    start_date: Optional[date] = None
    start_hm: Optional[str] = None  # "HH:MM"
    end_date: Optional[date] = None
    end_hm: Optional[str] = None
    reason: str = Field(min_length=1)


class DayCorrectionResponse(BaseModel):
    ok: bool = True
