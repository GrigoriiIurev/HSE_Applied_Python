from datetime import datetime
import uuid

from sqlalchemy import Column, String, TIMESTAMP, Boolean, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import UUID

from auth.db import Base


class Link(Base):
    __tablename__ = "links"

    id = Column(UUID, primary_key=True, default=uuid.uuid4, index=True)

    original_url = Column(String, nullable=False)

    short_code = Column(String, unique=True, index=True, nullable=False)

    custom_alias = Column(String, unique=True, nullable=True)

    created_at = Column(TIMESTAMP, default=datetime.utcnow)

    expires_at = Column(TIMESTAMP, nullable=True)

    click_count = Column(Integer, default=0)

    last_used_at = Column(TIMESTAMP, nullable=True)

    is_active = Column(Boolean, default=True)

    owner_id = Column(UUID, ForeignKey("users.id"), nullable=True)