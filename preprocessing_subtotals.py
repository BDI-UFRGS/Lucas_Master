import re
from glob import glob
from os.path import join, dirname

import pandas as pd
from owlready2 import get_ontology, ThingClass

MACRO_LOCATIONS = ['interstitial', 'framework', 'framework and interstitial']


def calculate_subtotals(target_path, idiom):
    dataset = pd.read_excel(target_path, index_col=[0], header=[0])

    petr_ont = get_ontology('petroledge_model.owl').load()

    feature_names = list(map(lambda x: x.lower(), list(dataset.columns)))
    feature_names = [re.sub('(\[.*\])', '', feature_name) for feature_name in feature_names]

    dataset.columns = feature_names

    others_names = {'grain_size', 'petrofacie', 'phi stdev sorting', 'sorting', 'porosity'}
    others_names = list(set(feature_names) & others_names)

    others = dataset[others_names]
    if 'sorting' in others_names:
        del others['sorting']
    dataset = dataset.drop(others_names, axis=1)

    feature_names = [feature_name for feature_name in feature_names if feature_name not in others_names]

    compositional_groups = [extract_mineral_group(petr_ont, column) for column in feature_names]
    locational_groups = [extract_locational_group(petr_ont, column) for column in feature_names]

    group_types = ['raw' for _ in feature_names] + ['compositional_groups' for _ in feature_names] + [
        'localizational_groups' for _ in feature_names]
    features_groups = feature_names + compositional_groups + locational_groups

    dataset = pd.concat([dataset, dataset, dataset], axis=1)
    dataset.columns = pd.MultiIndex.from_tuples(zip(group_types, features_groups, feature_names * 3))
    dataset.columns.names = ['top_level', 'features_groups', 'features']

    group_types = ['others' for _ in list(others.columns)]
    features_groups = list(others.columns)
    others.columns = pd.MultiIndex.from_tuples(zip(group_types, features_groups, list(others.columns)))
    others.columns.names = ['top_level', 'features_groups', 'features']

    dataset = pd.concat([dataset, others], axis=1)

    dataset.index.name = 'sample'
    dataset = dataset.sort_index(axis=1)

    dataset = dataset.fillna(value=0)

    if any(dataset.isna().any().values):
        print(dataset.isna().any())
        raise ValueError('There should not be any NaN values inside the subtotals data frame!')

    return dataset


def extract_mineral_group(ontology, column):
    if column.count(' - ') < 2:
        return 'others'

    constituent = column.split(' - ')[0].replace(' ', '_')

    location_class = ontology[constituent]
    if location_class is None:
        print(f'Group not found for {column}')
        return 'other'

    return location_class.is_a[0].name


def extract_locational_group(ontology, column):
    attributes = column.split(' - ')
    attributes = [attribute.replace(' ', '_') for attribute in attributes]
    n_attributes = column.count(' - ')

    if n_attributes == 2:
        location = attributes[1]
    elif n_attributes == 6:
        location = attributes[2]
    elif n_attributes == 5:
        location = attributes[1]
    else:
        return 'others'

    if location == '':
        print(f'Location empty for "{column}"')

    location_class: ThingClass = ontology.search_one(is_a=ontology['location'], iri=f'*{location}')
    if location_class is None:
        print(f'{location} NOT FOUND IN THE ONTOLOGY')
        return location
    high_level_parents = [ontology[loc] for loc in
                          ['location', 'diagenetic_location', 'porosity_location', 'primary_location']]

    parent_class: ThingClass = location_class.is_a[0]
    if parent_class not in high_level_parents:
        return parent_class.name
    else:
        return location_class.name


if __name__ == '__main__':
    idiom = 'ENUS'
    target_paths = glob('datasets/*/dataset_original.xlsx')
    target_pats = ['']

    for target_path in target_paths:
        print(f'processing {target_path}')
        subtotals_df = calculate_subtotals(target_path, idiom)
        target_save_path = join(dirname(target_path), 'subtotals_dataset2.xlsx')
        print(f'saving results to {target_save_path}')
        subtotals_df.to_excel(target_save_path)

    print('Done')
