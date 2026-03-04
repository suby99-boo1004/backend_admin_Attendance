from __future__ import annotations

from fastapi import APIRouter

# 관리자 API 루트: /api/admin/...
router = APIRouter(prefix="/api/admin", tags=["Admin"])


# 1) 관리자 근태관리(기존 기능)  -> /api/admin/attendance/...
from app.modules.admin_attendance.router import router as admin_attendance_router
router = APIRouter(prefix="/api/admin", tags=["Admin"])

# 2) 부서관리 -> /api/admin/departments
from app.modules.admin_departments.router import router as admin_departments_router
router.include_router(admin_departments_router)

# 3) 사용자관리 -> /api/admin/users
from app.modules.admin_users.router import router as admin_users_router
router.include_router(admin_users_router)



