#!/usr/bin/env python
#coding: utf-8

import tempfile, heapq, os, collections, itertools
from commons import ujson


# External sort code
# based on ActiveState Recipe 466302: Sorting big files the Python 2.4 way by Nicolas Lehuen &
# ActiveState Recipe 576755: Sorting big files the Python 2.6 way by Gabriel Genellina
# http://code.activestate.com/recipes/576755-sorting-big-files-the-python-26-way/
def merge(key, *iterables):
    # based on code posted by Scott David Daniels in c.l.p.
    # http://groups.google.com/group/comp.lang.python/msg/484f01f1ea3c832d

    Keyed = collections.namedtuple("Keyed", ["key", "obj"])

    keyed_iterables = [(Keyed(key(obj), obj) for obj in iterable) for iterable in iterables]
    for element in heapq.merge(*keyed_iterables):
        if element.key != (int('0xdbe928f86f85143c8282db0da081c05530ea2163', 16),):
            yield element.obj
        else:
            continue

def batch_sort(process_parameters, file_parameters):

    def sort_key(data):
        try:
            parsed_data = ujson.loads(data)
        except:
            return (int('0xdbe928f86f85143c8282db0da081c05530ea2163', 16),) # kludge: magic key indicates unparsable json
        return tuple([parsed_data[field] for field in process_parameters.sort_fields])


    tempdirs = file_parameters.temp_dirs

    if tempdirs is None:
        tempdirs = []
    if not tempdirs:
        tempdirs.append(tempfile.gettempdir())

    chunks = []
    try:
        with open(file_parameters.input_file,'rb',64*1024) as input_file:
            input_iterator = iter(input_file)
            for tempdir in itertools.cycle(tempdirs):
                current_chunk = list(itertools.islice(input_iterator,process_parameters.buffer_size))
                if not current_chunk:
                    break
                current_chunk.sort(key=sort_key)
                output_chunk = open(os.path.join(tempdir,'%06i'%len(chunks)),'w+b',64*1024)
                chunks.append(output_chunk)
                output_chunk.writelines(current_chunk)
                output_chunk.flush()
                output_chunk.seek(0)
        del current_chunk
        with open(file_parameters.sorted_file,'wb',64*1024) as output_file:
            output_file.writelines(merge(sort_key, *chunks))
    finally:
        for chunk in chunks:
            try:
                chunk.close()
                os.remove(chunk.name)
            except Exception:
                pass

