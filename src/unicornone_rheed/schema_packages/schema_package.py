from typing import (
    TYPE_CHECKING,
)

if TYPE_CHECKING:
    from structlog.stdlib import (
        BoundLogger,
    )
import h5py
from datetime import datetime
from nomad.config import config
from nomad.datamodel.data import ArchiveSection, EntryData, EntryDataCategory, Schema
from nomad.datamodel.hdf5 import HDF5Reference
from nomad.datamodel.metainfo.annotations import (
    ELNAnnotation,
    ELNComponentEnum,
    SectionProperties,
)
from nomad.datamodel.metainfo.basesections import (
    CompositeSystem,
    CompositeSystemReference,
    Experiment,
    Instrument,
    InstrumentReference,
    Measurement,
    Process,
    ProcessStep,
)
from nomad.datamodel.metainfo.plot import PlotlyFigure, PlotSection
from nomad.metainfo import Datetime, MEnum, Quantity, SchemaPackage, Section, SubSection
from nomad.metainfo.metainfo import Category, Reference, SectionProxy
from structlog.stdlib import BoundLogger
# from nomad_measurements.utils import HDF5Handler

configuration = config.get_plugin_entry_point(
    'unicornone_rheed.schema_packages:schema_package_entry_point'
)

m_package = SchemaPackage()

    
'''

The HDF file attribute meta data:

avg_frame_rate "10" >>> only really needed for rotation
chunk_size "50" >> total number of frames (also in dims)
data_id "4b6cc67b-9375-4818-90fd-81c07b107d43" >>> identifies datafiles that belong to
 a set
data_stream "rheed" >>> always rheed
dims "50,354,512" >>> dimensions of the 3D byte array
end_unix_ms_utc "1741616083821" >>> UNIX timestamp in milliseconds of last frame
is_rotating "0" >>> important in the future  for rotating samples
is_stream "1" >>> always 1
raw_frame_rate "10"  >>> only really needed for rotation
start_unix_ms_utc "1741616078554"  >>> UNIX timestamp in milliseconds of first frame

The dataset attribute meta data:

Name frames >>> always "frames"
Type Integer (unsigned), 8-bit, little-endian >> encoding
Shape 50 x 354 x 512 = 9062400 >>> total dims 
Chunk shape 1 x 354 x 512 = 181248 >>> single frame dims

Then the dataset itself is just a 3D byte array written directly into the file. 

The total frames can be any number from 1 to 50, depending on when the user clicks 
"stop". 
The data_id is a random generated hex string that corresponds to an AWS bucket in 
another implementation, with NOMAD I will just generate a random string.

The general idea is that once we have the files 50.h5, 100.h5, 150.h5, we can piece 
them back into a full video stream to play AND the model will have easily searchable 
data. 

copied from hdf5 file
group
Path	/
File path	50.h5
Raw	
Inspect
Attributes
avg_frame_rate	"10"
chunk_size	"50"
data_id	"362cfd0f-fa1b-48f7-b838-19e91e3be5d8"
data_stream	"rheed"
dims	"50,354,512"
end_unix_ms_utc	"1741617010313"
is_rotating	"0"
is_stream	"1"
raw_frame_rate	"10"
start_unix_ms_utc	"1741616987823"

missing:
- process reference
- sample reference
'''         


# class NewSchemaPackage(Schema):
#     name = Quantity(
#         type=str, a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity)
#     )
#     message = Quantity(type=str)

#     def normalize(self, archive: 'EntryArchive', logger: 'BoundLogger') -> None:
#         super().normalize(archive, logger)

#         logger.info('NewSchema.normalize', parameter=configuration.parameter)
#         self.message = f'Hello {self.name}!'
class Results(ArchiveSection):
    m_def = Section()
    name = Quantity(
        type=str, a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity,
                                      label='file_name')
    )
    data_id = Quantity(
        type=str, a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity)
    )
    start_time = Quantity(
        type=Datetime, a_eln=ELNAnnotation(component=ELNComponentEnum.DateTimeEditQuantity)
    )
    end_time = Quantity(
        type=Datetime, a_eln=ELNAnnotation(component=ELNComponentEnum.DateTimeEditQuantity)
    )
    frames = Quantity(
        type=HDF5Reference)
class MeasurementSettings(ArchiveSection):
    m_def = Section()
    avg_frame_rate = Quantity(
        type=int, a_eln=ELNAnnotation(component=ELNComponentEnum.NumberEditQuantity)
    )
    dimensions = Quantity(
        type=str, a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity)
    )
    chunk_size = Quantity(
        type=int, a_eln=ELNAnnotation(component=ELNComponentEnum.NumberEditQuantity)
    )
    is_rotating = Quantity(
        type=bool, a_eln=ELNAnnotation(component=ELNComponentEnum.BoolEditQuantity)
    )
    is_stream = Quantity(
        type=bool, a_eln=ELNAnnotation(component=ELNComponentEnum.BoolEditQuantity)
    )
    raw_frame_rate = Quantity(
        type=int, a_eln=ELNAnnotation(component=ELNComponentEnum.NumberEditQuantity)
    )
class RHEEEDMeasurement(Measurement, Schema):
    m_def = Section()
    name = Quantity(
        type=str, a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity)
    )
    datetime = Quantity(
        type=Datetime, a_eln=ELNAnnotation(component=ELNComponentEnum.DateTimeEditQuantity,
                                           label='start_time')
    )
    end_time = Quantity(
        type=Datetime, a_eln=ELNAnnotation(component=ELNComponentEnum.DateTimeEditQuantity,
        )
    )

    hdf5_file = Quantity(
        type=str, shape=['*'], a_eln=ELNAnnotation(component=ELNComponentEnum.FileEditQuantity)
    )
    measurement_settings = SubSection(
        section_def=MeasurementSettings
    )
    results = SubSection(
        section_def=Results,
        description="""
        The result of the measurement.
        """,
        repeats=True,
    )
    def normalize(self, archive, logger):
        
        # handler = HDF5Handler(self.hdf5_file,archive, logger)
        # frames = handler.read_dataset("/frames")
        # h5data = HDF5Reference.read_dataset(archive, self.hdf5_file + '#/frames')
        # self.results = []
        # for file in self.hdf5_file:
        #     frames = file + '#/frames'
        #     result = Results(frames=frames)
        #     self.results.append(result)
        # super().normalize(archive, logger)	    
        self.results = []
        for file in self.hdf5_file:
            #with h5py.File(file, 'r') as hdf:
            with h5py.File(archive.m_context.raw_file(file, 'rb')) as hdf:
            # with archive.m_context.raw_file(
            #     file,
            #     'r',
            #      ) as file:
            #    hdf = h5py.File(file, 'r')
                attributes = {}
                for key, value in hdf.attrs.items():
                    if isinstance(value, bytes):
                        attributes[key] = value.decode('utf-8')
                    elif isinstance(value, (int, float)):
                        attributes[key] = value
                    elif isinstance(value, (list, tuple)):
                        attributes[key] = [v.decode('utf-8') if isinstance(v, bytes) else v for v in value]
                    else:
                        attributes[key] = value

                # Convert UNIX timestamps to datetime
                start_time = datetime.utcfromtimestamp(int(attributes['start_unix_ms_utc'])/1000)
                end_time = datetime.utcfromtimestamp(int(attributes['end_unix_ms_utc'])/1000)
                #end_time = datetime.utcfromtimestamp(attributes['end_unix_ms_utc'] / 1000.0)

                result = Results(
                    name=file,
                    data_id=attributes['data_id'],
                    start_time=start_time,
                    end_time=end_time,
                    frames=file + '#/frames'
                )

                # Set measurement settings
                self.measurement_settings = MeasurementSettings(
                    avg_frame_rate=int(attributes['avg_frame_rate']),
                    dimensions=attributes['dims'],
                    chunk_size=int(attributes['chunk_size']),
                    is_rotating=bool(int(attributes['is_rotating'])),
                    is_stream=bool(int(attributes['is_stream'])),
                    raw_frame_rate=int(attributes['raw_frame_rate'])
                )

                self.results.append(result)


# class UnicornOneRHEEED(Experiment, Schema):
#     m_def = Section()
#     name = Quantity(
#         type=str, a_eln=ELNAnnotation(component=ELNComponentEnum.StringEditQuantity)
#     )
#     steps = SubSection(
#         section_def=RHEEEDMeasurement,
#         description="""
#         An ordered list of all the dependant steps that make up this activity.
#         """,
#         repeats=True,
#     )

m_package.__init_metainfo__()
