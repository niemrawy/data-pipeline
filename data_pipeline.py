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
            
        
        def sum(self, column):
            self._read_data()
            totals = (int(columns[column]) for columns in self.rows)
            return sum(totals)
            

