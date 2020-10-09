from collections import Counter


class DataPipeline():

    @staticmethod
    def read_csv(path):
        return DataPipeline.DataFrame(data_path=path, separator=",")

    
    class DataFrame():

        def __init__(self, data_path, separator):
            self.data_path = data_path
            self.separator = separator


        def _read_data(self):
            data = (line.rstrip("\n").split(self.separator) for line in open(self.data_path))
            self.columns = next(data)
            self.rows = (dict(zip(self.columns, row)) for row in data)
        

        def select(self, selected_columns):
            self.selected = selected_columns
            return self


        def sum(self):
            self._read_data()
            rows = self.selected if self.select else self.columns
            totals = Counter()
            for row in self.rows:
                for k in rows:
                    totals[k] += int(row[k])

            return dict(totals)