def determinePartition(username: str):
        """ Determines dataset partition to send table entry to.
        Sorts based on alphabetical order.
        """
        if username < "I":
            return 1
        elif username >= "I" and username < "Q":
            return 2
        else:
            return 3