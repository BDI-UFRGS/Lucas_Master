import os
from itertools import chain
from package.main import run


def main():
    run_multiple = 1
    db_file = 'tmp.db'
    args = {
        '-e': '0.1',
        '--num-gen': '1000',
        '--pop-size': '256',
        '--min-features': '2',
        '--max-features': '10000',
        '--strategy': 'ga',
        '--db-file': db_file,
        '--cluster-algorithm': 'agglomerative'
    }

    dataset_locations = {
        'dastaset_name': './dataset/path/'
    }

    for name, dataset_file in dataset_locations.items():
        if not os.path.isfile(dataset_file):
            raise FileNotFoundError(f'{dataset_file} not found for {name}')

    for scenario in [('raw',), ('compositional_groups', 'localizational_groups')]:
        local_args = args
        local_args['--scenario'] = list(scenario)
        fitness_metric = 'silhouette_sklearn'
        local_args['--fitness-metric'] = fitness_metric
        for experiment_name, dataset_file in dataset_locations.items():
            print(f'RUNNING {experiment_name} in scenario {str(scenario)}')

            for i in range(run_multiple):
                print(f'{i+1}/{run_multiple}')

                list_local_args = list(chain.from_iterable(local_args.items()))
                run(args=list_local_args + [dataset_file, experiment_name, '--multi-level'])


if __name__ == '__main__':
    main()
