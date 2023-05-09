class LocalDeduplicationManager:

    def __record_ids(self): return set()

    def does_not_exist(self, record_id):
        ret_val = False

        if record_id not in self.__record_ids():
            self.__record_ids().add(record_id)
            ret_val = True

        return ret_val

