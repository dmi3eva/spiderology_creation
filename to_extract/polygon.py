from atomic import *
from to_extract.sample import *
from to_extract.sample.zoo import *


if __name__ == "__main__":
    db = ""
    text = ""
    query = "SELECT name, title FROM city JOIN ON city.id = person.id JOIN book ON city.id = person.id"
    sample = Sample(db=db, nl=text, sql=query)
    species_1 = Paired_join()
    print(species_1.is_appropriate(sample))
