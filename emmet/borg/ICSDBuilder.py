# Usage:
# with multiprocessing:
#    python runner_sample.py
# with mpi(need mpi4py pacakge):
#    mpiexec -n 5 python runner_sample.py

import random
import time
import logging
import sys
import pymongo
from pymongo import UpdateOne
from maggma.stores import MongoStore
from maggma.builder import Builder
from maggma.runner import Runner
from maggma.runner import BaseProcessor
from pymatgen.analysis.structure_matcher import StructureMatcher, ElementComparator
from pymatgen.core.structure import Structure

class ICSDBuilder(Builder):

    def __init__(self,  sources, targets, get_chunk_size, process_chunk_size=1):
        super(ICSDBuilder, self).__init__(sources, targets, get_chunk_size,
                                            process_chunk_size)

    def get_items(self):
        items = []
        collec = sources[0].collection
        formulas = collec.distinct('formula_reduced_abc')
        for formula in formulas:
            cursor = collec.find({'formula_reduced_abc':formula},projection=['icsd_id','snl'],sort=[('icsd_id',1)])
            yield list(cursor)

    def process_item(self, item):
        m = StructureMatcher(
        ltol=0.2, stol=0.3, angle_tol=5, primitive_cell=True, scale=True,
        attempt_supercell=False, comparator=ElementComparator()
        )
        grouped = []
        for unmatched in item:
            matched = False
            for group in grouped:
                group_struc = Structure.from_dict(group[0]['snl'])
                unmatched_struc = Structure.from_dict(unmatched['snl'])
                if m.fit(group_struc,unmatched_struc):
                    matched = True
                    #print('{} under {}'.format(unmatched['icsd_id'],group[0]['icsd_id']))
                    group.append(unmatched)
                    break
            if not matched:
                grouped.append([unmatched])
        return grouped

    def update_targets(self, items):
        target = targets[0].collection
        requests = []
        for item in items:
            for group in item:
                doc = {}
                doc['icsd_id'] = group[0]['icsd_id']
                doc['all_icsd_ids'] = []
                for struc in group:
                    doc['all_icsd_ids'].append(struc['icsd_id'])
                requests.append(UpdateOne({'icsd_id':doc['icsd_id']},{'$addToSet':{'all_icsd_ids': {'$each':doc['all_icsd_ids']}}},upsert=True))
        target.bulk_write(requests)


    def finalize(self, cursor):
        print("Finalizing ...")

        # Close any Mongo connections.
        for store in (self.sources + self.targets):
            try:
                store.collection.database.client.close()
            except AttributeError:
                continue
        # Runner will pass iterable yielded by `self.get_items` as `cursor`. If
        # this is a Mongo cursor with `no_cursor_timeout=True` (not the
        # default), we must be explicitly kill it.
        try:
            cursor and cursor.close()
        except AttributeError:
            pass
        print("DONE!")

if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setLevel(logging.DEBUG)
    sh.setFormatter('%(asctime)s %(levelname)s %(message)s')
    logger.addHandler(sh)

    get_chunk_size = 1000
    process_chunk_size = 500


    sources = [MongoStore('ICSD','ICSD_files')]
    targets = [MongoStore('ICSD','DerivedCollec')]

    mdb = ICSDBuilder(sources, targets, get_chunk_size=get_chunk_size,
                        process_chunk_size=process_chunk_size)

    builders = [mdb]
    runner = Runner(builders)
    runner.run()
