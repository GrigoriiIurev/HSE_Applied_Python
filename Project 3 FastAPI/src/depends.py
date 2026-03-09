from fastapi import Depends
from auth.users import current_active_user, current_optional_user
from auth.db import User


async def get_current_user(user: User = Depends(current_active_user)):
    return user


async def get_optional_user(user: User | None = Depends(current_optional_user)):
    return user
