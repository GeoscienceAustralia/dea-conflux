"""Produce datasets for consumption.

Matthew Alger, Alex Leith
Geoscience Australia
2021
"""

import logging
from typing import Dict, Iterable

from datacube import Datacube
from datacube.model import Dataset

_log = logging.getLogger(__name__)


def find_datasets(
    query: Dict[str, str], products: [str], limit: int = None, dc: Datacube = None
) -> Iterable[Dataset]:
    """
    Find datasets with a Datacube query.

    Heavily based on datacube_alchemist.worker._find_datsets.

    Arguments
    ---------
    query : Dict[str, str]

    products : [str]
        List of products to search.

    limit : int
        Maximum number of datasets to return (default unlimited).

    dc : Datacube
        Datacube or None.

    Returns
    -------
    Generator of datasets
    """
    if dc is None:
        dc = Datacube()

    # Find many datasets across many products with a limit
    count = 0

    for product in products:
        datasets = dc.index.datasets.search(
            product=product,
            **query,
        )

        try:
            for dataset in datasets:
                yield dataset
                count += 1
                if limit is not None and count >= limit:
                    return
        except ValueError as error:
            _log.warning(
                f"Error searching for datasets. "
                f"Maybe none were returned? "
                f"Error was {error}"
            )
            continue
