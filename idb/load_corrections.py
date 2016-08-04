import os

from corrections.loader import CorrectionsLoader

from data_tables import locality, taxon, taxon_extended


def main():
    c = CorrectionsLoader()

    # for s in locality.get_sources():
    #     c.clear_source(s)

    # for s in taxon.get_sources():
    #     c.clear_source(s)

    for s in taxon_extended.get_sources():
        c.clear_source(s)

    with CorrectionsLoader() as loader:
        # for dr in locality.get_data():
        #     loader.add_corrections(dr[0], dr[1], dr[2], approved=dr[3])

        # for dr in taxon.get_data(os.path.expanduser("~/taxon/checklist1.zip")):
        #     loader.add_corrections(dr[0], dr[1], dr[2], approved=dr[3])

        loader.add_corrections_iter(taxon_extended.get_data())

if __name__ == '__main__':
    main()
