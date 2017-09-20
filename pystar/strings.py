

def normalize_strings(lines):
    parsed = []
    mlstring = None
    for line in lines:
        sline = line.lstrip()
        if len(sline) == 0:
            # skip empty lines
            continue
        elif sline[0] == ';':
            # we have start or end of a multi-line string
            if mlstring is None:
                # starting a new mlstring
                mlstring = []
            else:
                # closing current mlstring
                mlstring = ';' + '\n'.join(mlstring)
                parsed.append(mlstring)
                mlstring = None
        elif mlstring is not None:
            # we are in a mlstring, add the current line un-stripped
            mlstring.append(line)
        else:
            # not a multiline string, but normalize the line if it contains quoted strings
            for part in normalize_quoted_string(sline):
                parsed.append(part)
    return parsed


def normalize_quoted_string(line):
    '''
    most complicated function in the module.
    this disambiguates between quoted strings and comments
    all strings are normalized by being put on their own line with a starting `;`
    comments are dropped
    '''

    lidx = 0
    lines = []
    eidx = len(line)

    def findmax(line, char, lidx, eidx):
        idx = line.find(char, lidx)
        if idx < 0:
            return eidx
        return idx

    def add(part):
        if len(part.lstrip()):
            lines.append(part)

    while lidx < eidx:
        sidx = findmax(line, "'", lidx, eidx)
        didx = findmax(line, '"', lidx, eidx)
        cidx = findmax(line, '#', lidx, eidx)
        if sidx < min(didx, cidx):
            add(line[lidx:sidx])
            nidx = findmax(line, "'", sidx+1, eidx)
            add(';'+line[sidx+1:nidx])
            lidx = nidx+1
        elif didx < min(cidx, sidx):
            add(line[lidx:didx])
            nidx = findmax(line, '"', didx+1, eidx)
            add(';'+line[didx+1:nidx])
            lidx = nidx+1
        elif cidx < min(sidx, didx):
            add(line[lidx:cidx])
            lidx = eidx
        else:
            return [line]
    return lines
