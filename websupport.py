
def mkcombo(name, entries):
    return ('<select name="%s"><option value="null"></option>' % name
            + "".join(['<option value="%s">%s</option>' % (d["name"], d["desc"]) for d in entries])
            + '</select>')

