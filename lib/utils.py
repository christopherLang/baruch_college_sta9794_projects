import itertools as it


def simple_chunker(iterable, chunk_size=1000):
    """Evenly chunked iterator with generator

    Creates a generator that returns elements within the supplied iterator of
    approximately equal size. This avoids loading the whole iterator into
    memory (unless the iterable already exists in memory such as lists)

    Args:
        iterable (iterable):
            An object that is iterable, such as iterators, generators, etc.

        chunk_size (int):
            The size of each returned collection

    Returns (list(iterable)):
        The generator returns a list of elements retrieved from iterable of
        length chunk_size or less
    """
    chunking_iter = iter(iterable)
    element_block = list(it.islice(chunking_iter, chunk_size))

    while True:
        element_block = list(it.islice(chunking_iter, chunk_size))

        if len(element_block) != 0:
            yield element_block

        else:
            break


def sliding_chunker(iterable, chunk_size=1000, inc=2):
    """Sliding window chunked iterator with generator

    Creates a generator that returns elements within the supplied iterator of
    of length chunk_size. Each subsequent call will shift the window forward by
    inc while maintaining length chunk_size

    Args:
        iterable (iterable):
            An object that is iterable, such as iterators, generators, etc.

        chunk_size (int):
            The size of each returned collection

        inc (int):
            Sliding window increment size

    Returns (list(iterable)):
        The generator returns a list of elements retrieved from iterable of
        length chunk_size or less, shifted by inc forward (except first call)
    """
    chunking_iter = iter(iterable)
    element_block = list(it.islice(chunking_iter, chunk_size))

    if len(element_block) == chunk_size:
        yield element_block

    counting_index = 0
    inc_elements = list()
    for an_element in chunking_iter:
        inc_elements += [an_element]
        counting_index += 1

        if (counting_index % inc) is 0:
            element_block = element_block[inc:] + inc_elements

            yield element_block

            counting_index = 0
            inc_elements = list()
