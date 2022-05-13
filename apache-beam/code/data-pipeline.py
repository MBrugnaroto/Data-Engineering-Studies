import re
import apache_beam as Beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from utils.constants.main import path_sample_dengue, path_sample_rains
from utils.constants.main import path_result, header


def getColuns(file, sep='|'):
    with open(file, "r") as file:
        header = splitFields(file.readline(), sep)
    return header


def instantiateBeamPipeline():
    pipeline_options = PipelineOptions(argv=None)
    return Beam.Pipeline(options=pipeline_options)


def splitFields(element, sep='|'):
    return element.replace('\n', '').split(sep)


def listToDict(listelements, coluns):
    return dict(zip(coluns, listelements))


def treatsDate(element, key):
    element['ano-mes'] = '-'.join(splitFields(element[key], '-')[:2])
    return element


def setDictAsTupla(dict_, key):
    uf = dict_.pop(key, None)
    return (uf, dict_)


def casesByUFDate(element):
    uf, registers = element

    for register in registers:
        value = float(register['casos']) if isDigit(register['casos']) else (
                                            f"{uf}-{register['ano-mes']}", 0.0
                                            )
        yield (f"{uf}-{register['ano-mes']}", value)


def mmByUFDate(element):
    data, mm, uf = element
    year_month = '-'.join(data.split('-')[:2])
    key = f'{uf}-{year_month}'
    mm_checked = float(mm) if float(mm) > 0 else 0.0
    return (key, mm_checked)


def isDigit(string):
    return True if re.compile(r'\d+').match(string) else False


def roundMM(element):
    key, value = element
    return (key, round(value, 1))


def filterInvalidRows(element):
    key, value = element

    if all([value['rains'], value['dengue']]):
        return True
    return False


def unzipElements(element):
    key, value = element
    uf, year, month = key.split('-')
    return (uf, year, month, *sum(value.values(), []))


def listToString(element, sep):
    return f"{sep}".join(str(value) for value in element)


global coluns_dataset_dengue
coluns_dataset_dengue = getColuns(path_sample_dengue)


with instantiateBeamPipeline() as pipeline:
    dengue = (
        pipeline
        | "Dengue Dataset Reader" >> ReadFromText(path_sample_dengue,
                                                  skip_header_lines=1)
        | "Dengue records to List" >> Beam.Map(splitFields)
        | "Dengue records to dict" >> Beam.Map(listToDict,
                                               coluns=coluns_dataset_dengue)
        | "Dengue: setting ano-mes columns" >> Beam.Map(treatsDate,
                                                        key='data_iniSE')
        | "Dengue: setting UF as key" >> Beam.Map(setDictAsTupla, key='uf')
        | "Dengue: group records by UF" >> Beam.GroupByKey()
        | "Dengue: cases by UF-DATE" >> Beam.FlatMap(casesByUFDate)
        | "Dengue: group by UF-DATE" >> Beam.CombinePerKey(sum)
    #   | "Dengue: show cases total by UF-DATE" >> Beam.Map(print)
    )

    rains = (
        pipeline
        | "Rains dataset reader" >> ReadFromText(path_sample_rains,
                                                 skip_header_lines=1)
        | "Rain records to list" >> Beam.Map(splitFields, sep=',')
        | "Rain: MM by UF-DATE" >> Beam.Map(mmByUFDate)
        | "Rain: grouby records by UF-DATE" >> Beam.CombinePerKey(sum)
        | "Rain: round MM" >> Beam.Map(roundMM)
    #   | "Rain: show MM total by UF-DATE" >> Beam.Map(print)
    )

    pcollections_result = (
    #   ---- Merge pcollections with Flatten
    #   (rains, dengue)
    #   | "Merge pcollections" >> Beam.Flatten()
    #   | "Group by Key" >> Beam.GroupByKey()
    #   | "Show merged pcollections" >> Beam.Map(print)

    # ---- Merge pcollections with CoGroupyByKey
        ({'rains': rains, 'dengue': dengue})
        | "Merge pcollections" >> Beam.CoGroupByKey()
        | "Filter invalid rows" >> Beam.Filter(filterInvalidRows)
        | "Unzip rows" >> Beam.Map(unzipElements)
        | "List to String" >> Beam.Map(listToString, sep=";")
    #   | "Show merged and filtered pcollections" >> Beam.Map(print)
    )

    pcollections_result | " Write result to CSV file" >> (
            WriteToText(file_path_prefix=path_result+'result',
                        file_name_suffix='.csv',
                        header=header))
