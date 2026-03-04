# 관리자 근태관리(리포트) 모듈 추가 안내

## 1) 새로 추가되는 API
- GET  /api/admin/attendance/settings
- PUT  /api/admin/attendance/settings
- GET  /api/admin/attendance/report?period=month&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD&user_id=

## 2) DB 가정(필요시 수정)
- users(id, name, is_active, is_admin)
- work_sessions(user_id, start_at, end_at, work_type, work_status, place, note)
- settings(key, value_json)  ← 없으면 생성 필요
- leave_entitlements(user_id, annual_leave_total) ← 없으면 생성하면 좋음(없어도 기본 15로 동작)

## 3) 자동퇴근 인정 로직(리포트 계산용)
- end_at이 NULL이면:
  - 주간: work_date의 18:30으로 인정
  - 야간: work_date+1의 06:00으로 인정
- 실제 '확정'은 설정한 cutoff 시각에 일괄 처리(스케줄러)하는 것이 정석이며, 다음 단계에서 반영하면 됨.

## 4) 다음 단계 추천
- (중요) 실제 DB에 "자동 퇴근 확정" 배치 작업 추가 (cron/apscheduler)
- (중요) 외근(장소/내용)을 work_sessions 또는 별도 테이블로 표준화
- 월차/반차/휴일 근무 판정 기준을 회사 룰에 맞게 명확히 고정
