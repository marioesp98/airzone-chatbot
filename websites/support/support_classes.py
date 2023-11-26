class Subunit:
    def __init__(self, name):
        self.name = name
        self.description = None

    def add_description(self, description):
        self.description = description


class Unit:
    def __init__(self, name):
        self.name = name
        self.subunits = []

    def add_subunit(self, subunit):
        self.subunits.append(subunit)


class Category:
    def __init__(self, name):
        self.name = name
        self.units = []

    def add_unit(self, unit):
        self.units.append(unit)

    def add_units(self, units):
        self.units.extend(units)
