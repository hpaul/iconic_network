from models import *
import dask
import itertools


class Colour:
    OKGREEN = '\033[92m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


def save_author(id, data):
    try:
        # Check first if author exist
        coll = AuthorDetails.get(AuthorDetails.id == id)
    except AuthorDetails.DoesNotExist:
        with db.atomic() as transaction:
            name = str(data.get('surname')) + ' ' + str(data.get('given-name'))

            ath = AuthorDetails(id=id)
            ath.full_name = name
            ath.preferred_name = data.get('authname')
            ath.affiliation_id = data.get('afid')
            ath.url = data.get('author-url')

            try:
                ath.save(force_insert=True)
                print(".", end="")
            except IntegrityError:
                # Because this block of code is wrapped with "atomic", a
                # new transaction will begin automatically after the call
                # to rollback().
                transaction.rollback()


def save_network(from_author, to_author, data):
    with db.atomic() as transaction:
        print(data.get('keywords'))

        vertex = Network()
        vertex.from_author = from_author
        vertex.to_author = to_author
        vertex.article = data.abs_id
        vertex.year = data.published.year
        vertex.citations = data.cited_by

        try:
            vertex.save(force_insert=True)
            print(".", end="")
        except IntegrityError:
            # Because this block of code is wrapped with "atomic", a
            # new transaction will begin automatically after the call
            # to rollback().
            transaction.rollback()

def save_collaboration(row):
    lazy_network = []
    
    print(Colour.BOLD + str(row.abs_id) + ' - Saving authors' + Colour.END)
    # Store each author details
    for author in row.authors:
        id = author.get('authid')

        if id is not None:
            s_author = dask.delayed(save_author)(id, author)
            lazy_network.append(s_author)

    print('')
    print(Colour.OKGREEN + str(row.abs_id) + ' - Saved' + Colour.END)

    print(Colour.BOLD + str(row.abs_id) + ' - Saving network of authors' + Colour.END)
    auth_ids = [ath.get('authid') for ath in row.authors if ath.get('authid') is not None]
    # Generate network between article authors
    comb = itertools.combinations(auth_ids, 2)

    for from_author,to_author in comb:
        s_network = dask.delayed(save_network)(from_author, to_author, row)
        lazy_network.append(s_network)

    print('')
    print(Colour.OKGREEN + str(row.abs_id) + ' - Saved' + Colour.END)

    fetched = Collaboration.update(saved=True).where(Collaboration.abs_id == row.abs_id)
    fetched.execute()

    # Save network
    dask.compute(*lazy_network)


def build_network():
    lazy_collaborations = []
    collaborations = Collaboration.select().where(Collaboration.cited_by > 10, Collaboration.keywords is not None).limit(10000)

    for collaboration in collaborations:
        task = dask.delayed(save_collaboration)(collaboration)
        lazy_collaborations.append(task)

    results = dask.compute(*lazy_collaborations)

    return results

net = build_network()
print(net)
