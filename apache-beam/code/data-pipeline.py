import os
import os.path
import apache_beam as Beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from utils.constants.main import path_source_dengue

def getColuns(file, sep='|'):
    with open(file, "r") as file:
        header = splitFields(file.readline(), sep)
    return header

def instantiateBeamPipeline():
    pipeline_options = PipelineOptions(argv=None)
    return Beam.Pipeline(options=pipeline_options)

def splitFields(element, sep='|'):
    return element.split(sep)

def listToDict(listelements, coluns):
    return dict(zip(coluns, listelements))

def treatsDate(element):
    element['ano-mes'] = '-'.join(splitFields(element['data_iniSE'], '-')[:2])
    return element

def ufKey(element):
    uf = element.pop('uf', None)
    return (uf, element)

global coluns_dataset_dengue
coluns_dataset_dengue = getColuns(path_source_dengue)

with instantiateBeamPipeline() as pipeline:
    dengue = (
        pipeline
        | "Dengue Dataset Reader" >> 
            ReadFromText(path_source_dengue, 
                        skip_header_lines=1)
        | "String to List" >>
            Beam.Map(splitFields)
        | "Register to dict" >>
            Beam.Map(listToDict, coluns=coluns_dataset_dengue)
        | "Setting ano-mes columns" >>
            Beam.Map(treatsDate)
        | "Setting UF as key" >>
            Beam.Map(ufKey)
        | "Group by UF" >>
            Beam.GroupByKey()
        | "Show results" >>
            Beam.Map(print)
    )

# if __name__ == '__main__':
#     print(getColuns(path_source_dengue, '|'))