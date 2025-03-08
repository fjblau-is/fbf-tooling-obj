from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

# Database connection string
# Database should already exist
DB_URI = "postgresql://postgres:2Ellbelt!@localhost:5432/orm"

# Create SQLAlchemy engine
engine = create_engine(DB_URI)

# Create a Base class for ORM models
Base = declarative_base()

### **Process Table**
class Process(Base):
    __tablename__ = 'process'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    
    # Relationships
    frames = relationship("Frame", secondary="process_frame", back_populates="processes")
    objects = relationship("Object", secondary="process_object", back_populates="processes")

class Hazard(Base):
    __tablename__ = 'hazard'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    object_id = Column(Integer, ForeignKey('objects.id'), nullable=False)
    
    # Relationships
    objects = relationship("Object", back_populates="hazards")

### **Frame Table**
class Frame(Base):
    __tablename__ = 'frame'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)

    # Relationships
    processes = relationship("Process", secondary="process_frame", back_populates="frames")
    objects = relationship("Object", secondary="frame_object", back_populates="frames")

### **Object Table**
class Object(Base):
    __tablename__ = 'objects'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)

    # Relationships
    processes = relationship("Process", secondary="process_object", back_populates="objects")
    frames = relationship("Frame", secondary="frame_object", back_populates="objects")

### **Process_Frame Association Table (Many-to-Many)**
class ProcessFrame(Base):
    __tablename__ = 'process_frame'
    process_id = Column(Integer, ForeignKey('process.id'), primary_key=True)
    frame_id = Column(Integer, ForeignKey('frame.id'), primary_key=True)

### **Process_Object Association Table (Many-to-Many)**
class ProcessObject(Base):
    __tablename__ = 'process_object'
    process_id = Column(Integer, ForeignKey('process.id'), primary_key=True)
    object_id = Column(Integer, ForeignKey('objects.id'), primary_key=True)

### **Frame_Object Association Table (Many-to-Many)**
class FrameObject(Base):
    __tablename__ = 'frame_object'
    frame_id = Column(Integer, ForeignKey('frame.id'), primary_key=True)
    object_id = Column(Integer, ForeignKey('objects.id'), primary_key=True)

# Create the tables in the database
Base.metadata.create_all(engine)

# Create a session factory
Session = sessionmaker(bind=engine)
session = Session()
