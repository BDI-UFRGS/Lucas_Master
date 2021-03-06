from enum import Enum

from package.evaluation_functions import CLUSTER_CRIT_ALLOWED_FITNESSES
from package.main import run, e_scenarios


class e_datasets(Enum):
    CAMPUS_BASIN = './datasets/CampusBasin/subtotals_dataset2.xlsx'
    EQUATORIAL_MARGIN = './datasets/MargemEquatorial/subtotals_dataset2.xlsx'
    TALARA_BASIN = './datasets/TalaraBasin/subtotals_dataset2.xlsx'
    CARMOPOLIS_GROUPED = './datasets/CarmopolisGrouped/subtotals_dataset2.xlsx'
    JEQUITINHONHA = './datasets/Jequitinhonha/subtotals_dataset2.xlsx'
    MUCURI = './datasets/Mucuri/subtotals_dataset2.xlsx'


class e_meta_clustering_algorithms(Enum):
    GA = 'ga'
    PSO = 'pso'
    WARD_P = 'ward_p'
    RANDOM_GA = 'random_ga'
    NONE = 'none'


class e_clustering_algorithms(Enum):
    AGGLOMERATIVE = 'agglomerative'
    KMEANS = 'kmeans'
    AFFINITY_PROPAGATION = 'affinity-propagation'


PREFERENCES = {
    e_scenarios.RAW: {
        e_datasets.CAMPUS_BASIN: -650,
        e_datasets.EQUATORIAL_MARGIN: -400,
        e_datasets.TALARA_BASIN: -400,
        e_datasets.CARMOPOLIS_GROUPED: -540,
        e_datasets.JEQUITINHONHA: -470,
        e_datasets.MUCURI: -5300
    },
    e_scenarios.COMPOSITIONAL_LOCALIZATIONAL: {
        e_datasets.CAMPUS_BASIN: -1550,
        e_datasets.EQUATORIAL_MARGIN: -900,
        e_datasets.TALARA_BASIN: -750,
        e_datasets.CARMOPOLIS_GROUPED: -1590,
        e_datasets.JEQUITINHONHA: -900,
        e_datasets.MUCURI: -7700
    }
}


def run_experiment(args):
    run(args=args)


if __name__ == '__main__':
    database = 'section_5_1.db'
    for algorithm in e_clustering_algorithms:
        if algorithm != e_clustering_algorithms.KMEANS:
            continue
        for _ in range(200):
            for dataset in e_datasets:
                for scenario in e_scenarios:
                    input_args = [
                        dataset.value,
                        '1',
                        '--level', 'features_groups',
                        # '--num-gen', '0',
                        # '--pop-size', '0',
                        # '--perfect',
                        '--eval-rate', '1',
                        '--min-features', '50',
                        '--fitness-metric', 'silhouette_sklearn',
                        '--cluster-algorithm', f'{algorithm.value}',
                        '--db-file', f'{database}',
                        '--strategy', 'none',
                        # '--p_ward', '0',
                        '--preference', str(PREFERENCES[scenario][dataset]),
                        f'--scenario', scenario.name
                    ]
                    run_experiment(input_args)

    database = 'section_5_2.db'
    for affinity, linkage in [('euclidean', 'complete'), ('euclidean', 'single'),
                              ('manhattan', 'complete'), ('manhattan', 'single'),
                              ('euclidean', 'ward')]:
        for _ in range(10):
            for scenario in e_scenarios:
                for dataset in e_datasets:
                    input_args = [
                        dataset.value,
                        '1',
                        '--level', 'features_groups',
                        '--num-gen', '1000',
                        '--pop-size', '50',
                        '--perfect',
                        '--eval-rate', '0',
                        '--min-features', '2',
                        '--fitness-metric', 'silhouette_sklearn',
                        '--cluster-algorithm', 'agglomerative',
                        '--db-file', database,
                        '--strategy', 'ga',
                        # '--p_ward', '0',
                        # '--preference', str(PREFERENCES[scenario][dataset]),
                        f'--scenario', scenario.name,
                        '--max-gens-without-improvement', '200',
                        '--affinity', affinity,
                        '--linkage', linkage
                    ]
                    run_experiment(input_args)

    database = 'results_5_3.db'
    for metric, _ in CLUSTER_CRIT_ALLOWED_FITNESSES:
        for dataset in e_datasets:
            for scenario in e_scenarios:
                input_args = [
                    dataset.value,
                    '2',
                    '--level', 'features_groups',
                    '--num-gen', '1000',
                    '--pop-size', '50',
                    # '--perfect',
                    '--eval-rate', '0',
                    '--min-features', '2',
                    f'--fitness-metric', f'{metric}',
                    f'--cluster-algorithm', 'agglomerative',
                    f'--db-file', f'{database}',
                    '--strategy', 'ga',
                    # '--p_ward', '0',
                    # '--preference', '0',
                    f'--scenario', f'{scenario.name}',
                    '--max-gens-without-improvement', '200',
                    '--linkage', 'ward',
                ]
                run_experiment(input_args)
