import pandas as pd


class TwoRavenProfiler(object):
    def __init__(self):
        pass

    def profile(self, inputs: pd.DataFrame, metadata: dict) -> dict:
        """Save metadata json to file.

        Args:
            inputs: pandas dataframe
            metadata: dict

        Returns:
            dict
        """

        # TODO: augment metadata by the two_raven api, if possible

        return metadata
