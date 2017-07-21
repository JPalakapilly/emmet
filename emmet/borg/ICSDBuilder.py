# Usage:
# with multiprocessing:
#    python runner_sample.py
# with mpi(need mpi4py pacakge):
#    mpiexec -n 5 python runner_sample.py

import random
import time
import logging
import sys

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
        print('Getting Items')
        for db in sources:
            items += db.collection.find()
        print('Items Got')
        return items


    def process_item(self, item):
        struc = Structure.from_dict(item['snl'])
        print('Processing')
        m = StructureMatcher(
        ltol=0.2, stol=0.3, angle_tol=5, primitive_cell=True, scale=True,
        attempt_supercell=False, comparator=ElementComparator()
        )

        matched = False
        tcol = targets[0].collection
        for doc in tcol.find({'formula_reduced_abc':item['formula_reduced_abc']},projection={'icsd_id':True,'snl':True,'formula_reduced_abc':True,'all_icsd_ids':True}):
            if m.fit(struc,Structure.from_dict(doc['snl'])):
                matched = True
                if item['icsd_id'] in doc['all_icsd_ids']:
                    print('{} already in target collection.(Under {})'.format(item['icsd_id'],doc['icsd_id']))
                else:
                    tcol.update_one({'icsd_id':doc['icsd_id']},{'$push':{'all_icsd_ids':item['icsd_id']}})
                    print('{} filed under {}'.format(item['icsd_id'],doc['icsd_id']))
                break

        if not matched:
            item['all_icsd_ids'] = [item['icsd_id']]
            tcol.insert_one(item)


    def update_targets(self, items):
        pass
        # print("Received {} processed items".format(len(items)))

    def finalize(self):
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

    N = 10
    get_chunk_size = 3
    process_chunk_size = 2


    sources = [MongoStore('ICSD','ICSD_files')]
    targets = [MongoStore('ICSD','DerivedCol')]

    mdb = ICSDBuilder(sources, targets, get_chunk_size=get_chunk_size,
                        process_chunk_size=process_chunk_size)

    builders = [mdb]
    runner = Runner(builders)
    runner.run()
