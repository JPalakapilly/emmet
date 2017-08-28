from pymongo import MongoClient
from pymatgen import Composition
from pymatgen.matproj.snl import StructureNL
from pymatgen import MPRester
from pymatgen.core.structure import Structure

#db connections
dbhost = 'localhost'
dbport = 27017
dbname = 'ICSD'

client = MongoClient(dbhost,dbport)
db = client[dbname]
icsd_col = db['ICSD_files']
deriv_col = db['DerivedCollec']

def get_meta_from_structure(structure):
    """Used by `structure_to_mock_job`, to "fill out" a job document."""
    comp = structure.composition
    elsyms = sorted(set([e.symbol for e in comp.elements]))
    meta = {'nsites': len(structure),
            'elements': elsyms,
            'nelements': len(elsyms),
            'formula': comp.formula,
            'reduced_cell_formula': comp.reduced_formula,
            'reduced_cell_formula_abc': Composition(comp.reduced_formula)
            .alphabetical_formula,
            'anonymized_formula': comp.anonymized_formula,
            'chemsystem': '-'.join(elsyms),
            'is_ordered': structure.is_ordered,
            'is_valid': structure.is_valid()}
    return meta

def snl_to_mock_job(snl_dict):
    # Needs at least one author. This is for a mock job, so can put whatever.
    snl = StructureNL.from_dict(snl_dict)
    job = snl_dict
    if 'is_valid' not in job: job.update(get_meta_from_structure(snl.structure))
    sorted_structure = snl.structure.get_sorted_structure()
    job.update(sorted_structure.as_dict())
    return job



def job_is_submittable(job):

    # mpworks.processors.process_submissions.SubmissionProcessor#submit_new_workflow
    MAX_SITES = 200 # SubmissionProcessor.MAX_SITES above

    # from mpworks.workflows.wf_utils import NO_POTCARS
    NO_POTCARS = ['Po', 'At', 'Rn', 'Fr', 'Ra', 'Am', 'Cm', 'Bk', 'Cf', 'Es', 'Fm', 'Md', 'No', 'Lr']

    snl = StructureNL.from_dict(job)
    if len(snl.structure.sites) > MAX_SITES:
        print('REJECTED WORKFLOW FOR {} - too many sites ({})'.format(
            snl.structure.formula, len(snl.structure.sites)))
    elif not job['is_valid']:
        print('REJECTED WORKFLOW FOR {} - invalid structure (atoms too close)'.format(
            snl.structure.formula))
    elif len(set(NO_POTCARS) & set(job['elements'])) > 0:
        print('REJECTED WORKFLOW FOR {} - invalid element (No POTCAR)'.format(
            snl.structure.formula))
    elif not job['is_ordered']:
        print('REJECTED WORKFLOW FOR {} - invalid structure (disordered)'.format(
            snl.structure.formula))
    else:
        return True
    return False

def get_snl_from_id(icsd_id):
    return icsd_col.find_one({'icsd_id':icsd_id},projection=['snl'])['snl']




if __name__ == '__main__':
    submittables = []

    for s in deriv_col.find(projection=['icsd_id']):
        if job_is_submittable(snl_to_mock_job(get_snl_from_id(s['icsd_id']))):
            submittables.append(s['icsd_id'])

    mpr = MPRester(api_key='sNxknEySUTz2owRL')
    new_structures = []
    already_checked = []
    with open('submittable_icsd_ids') as f:
        for line in f:
            already_checked.append(int(line))

    with open('submittable_icsd_ids','a') as f:
        new_structures = []
        to_check = [x for x in submittables if x not in already_checked]
        print('submittables: {}'.format(len(submittables)))
        print('Already Checked: {}'.format(len(already_checked)))
        print('To Check: {}'.format(len(to_check)))
        for i,s in enumerate(to_check):
            struc = StructureNL.from_dict(icsd_col.find_one({'icsd_id':s},projection=['snl'])['snl']).structure
            found = mpr.find_structure(struc)
            if len(found) == 0:
                new_structures.append(s)
                f.write('{}\n'.format(str(s)))
        if len(new_structures):
            print("DONE!")
