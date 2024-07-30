from __future__ import annotations
from typing import List

import enum
from sqlalchemy import (
    create_engine,
    Column,
    Enum,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.orm import (
    Mapped,
    mapped_column,
    DeclarativeBase,
    relationship,
    sessionmaker,
)

class Base(DeclarativeBase):
    pass


class Parent(Base):
    __tablename__ = "parent_table"

    id: Mapped[int] = mapped_column(primary_key=True)
    candidates: Mapped[List["Child"]] = relationship("Child", back_populates="candidate_parent", overlaps="twin_parent")
    twins: Mapped[List["Child"]] = relationship("Child", back_populates="twin_parent", overlaps="candidates")


class Child(Base):
    __tablename__ = "child_table"

    id: Mapped[int] = mapped_column(primary_key=True)
    parent_id: Mapped[int] = mapped_column(ForeignKey("parent_table.id"))

    candidate_parent: Mapped[Parent] = relationship("Parent", back_populates="candidates")
    twin_parent: Mapped[Parent] = relationship("Parent", back_populates="twins")


# Enum to represent path kinds
class PathKind(enum.Enum):
    KIND1 = 'kind1'
    KIND2 = 'kind2'

# Enum to represent relationship types
class RelationshipType(enum.Enum):
    CANDIDATE = 'candidate'
    TWIN = 'twin'

class Path(Base):
    __tablename__ = 'paths'
    id = Column(Integer, primary_key=True)
    path = Column(String, nullable=False)
    kind = Column(Enum(PathKind), nullable=False)

    def __repr__(self):
        return f"Path(id={self.id}, path={self.path}, kind={self.kind})"

class Relationship(Base):
    __tablename__ = 'relationships'
    id = Column(Integer, primary_key=True)
    type = Column(Enum(RelationshipType), nullable=False)
    path1_id = Column(Integer, ForeignKey('paths.id'), nullable=False)
    path2_id = Column(Integer, ForeignKey('paths.id'), nullable=False)

    path1 = relationship('Path', foreign_keys=[path1_id])
    path2 = relationship('Path', foreign_keys=[path2_id])

    def __repr__(self):
        return f"Relationship(id={self.id}, type={self.type}, path1={self.path1}, path2={self.path2})"


if __name__ == "__main__":

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    # parent = Parent()
    # child1 = Child()
    # child2 = Child()

    # parent.candidates.append(child1)
    # parent.twins.append(child2)

    # session.add(parent)
    # session.commit()

    # # Verify the results
    # for parent in session.query(Parent).all():
    #     print(f"Parent ID: {parent.id}")
    #     print("Candidates:")
    #     for child in parent.candidates:
    #         print(f"  Child ID: {child.id}")

    #     print("Twins:")
    #     for child in parent.twins:
    #         print(f"  Child ID: {child.id}")

    # session.close()

# Add some paths
    path1 = Path(path='/path/to/file1', kind=PathKind.KIND1)
    path2 = Path(path='/path/to/file2', kind=PathKind.KIND2)
    path3 = Path(path='/path/to/file3', kind=PathKind.KIND1)

    session.add_all([path1, path2, path3])
    session.commit()

# Add relationships
    relationship1 = Relationship(type=RelationshipType.CANDIDATE, path1_id=path1.id, path2_id=path2.id)
    relationship2 = Relationship(type=RelationshipType.TWIN, path1_id=path1.id, path2_id=path3.id)

    session.add_all([relationship1, relationship2])
    session.commit()

# Query relationships
    for rel in session.query(Relationship).all():
        print(rel)

# Query paths
    for path in session.query(Path).all():
        print(path)
