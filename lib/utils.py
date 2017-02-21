import itertools as it


def row_chunker(iterator, chunk_size=1000):
    chunking_iter = iter(iterator)

    while True:
        element_block = list(it.islice(chunking_iter, chunk_size))

        if len(element_block) != 0:
            yield element_block

        else:
            break
