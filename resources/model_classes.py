class Product:
    def __init__(self, name, p_n='N/A', ean='N/A', description='N/A'):
        self.name = name
        self.p_n = p_n
        self.ean = ean
        self.description = description

    def add_p_n(self, p_n):
        self.p_n = p_n

    def add_ean(self, ean):
        self.ean = ean
    def add_description(self, description):
        self.description = description


class Subunit:
    def __init__(self, name, description=''):
        self.name = name
        self.description = description
        self.products = []

    def add_description(self, description):
        self.description = description

    def add_product(self, product):
        self.products.append(product)

    def add_products(self, products):
        self.products.extend(products)


class Unit:
    def __init__(self, name, subunit=None):
        self.name = name
        self.subunits = []
        if subunit is not None:
            self.subunits.append(subunit)

    def add_subunit(self, subunit):
        self.subunits.append(subunit)

    def add_subunits(self, subunits):
        self.subunits.extend(subunits)

class Category:
    def __init__(self, name, unit=None):
        self.name = name
        self.units = []
        if unit is not None:
            self.units.append(unit)

    def add_unit(self, unit):
        self.units.append(unit)

    def add_units(self, units):
        self.units.extend(units)
