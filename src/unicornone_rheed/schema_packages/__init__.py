from nomad.config.models.plugins import SchemaPackageEntryPoint
from pydantic import Field


class UnicornOneRHEEEDEntryPoint(SchemaPackageEntryPoint):
    parameter: int = Field(0, description='Custom configuration parameter')

    def load(self):
        from unicornone_rheed.schema_packages.schema_package import m_package

        return m_package


schema_package_entry_point = UnicornOneRHEEEDEntryPoint(
    name='UnicornOneRHEEED',
    description='New schema package entry point configuration.',
)
