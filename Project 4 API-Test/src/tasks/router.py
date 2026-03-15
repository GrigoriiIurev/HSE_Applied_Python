from fastapi import APIRouter, BackgroundTasks
from .tasks import remove_expired_links, remove_unused_links

router = APIRouter(
    prefix="/tasks",
    tags=["Tasks"]
)


@router.post("/cleanup-expired")
async def cleanup_expired(background_tasks: BackgroundTasks):
    background_tasks.add_task(remove_expired_links)

    return {
        "status": "cleanup started",
        "task": "remove expired links"
    }


@router.post("/cleanup-unused")
async def cleanup_unused(background_tasks: BackgroundTasks):
    background_tasks.add_task(remove_unused_links)

    return {
        "status": "cleanup started",
        "task": "remove unused links"
    }
