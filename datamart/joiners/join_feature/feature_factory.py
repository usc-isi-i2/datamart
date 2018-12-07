
from datamart.joiners.join_feature.feature_classes import *


class ColumnFactory:
    @staticmethod
    def create(dfw: DataFrameWrapper, indexes):
        """
        TODO: dynamically generate subclass of FeatureBase, by profiled info, datatype etc.
        Args:
            dfw:
            indexes:

        Returns:

        """
        if True:
            return NonCategoricalNumberFeature(dfw, indexes)


# class PetFactory:
#     def __init__(self):
#         pass
#
#     def acquire_dog(self):
#         return Dog()
#
#     def acquire_cat(self):
#         return Cat()
#
#     def acquire_pet_by_name(self, pet_type):
#         if pet_type == "dog":
#             return Dog()
#         elif pet_type == "cat":
#             return Cat()

