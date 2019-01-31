from datamart.joiners.rltk_joiner import RLTKJoiner


class ExactMatchJoiner(RLTKJoiner):
    # TODO: work around for demo, should implement a better one

    def __init__(self):
        super().__init__()
        self.exact_match = True

