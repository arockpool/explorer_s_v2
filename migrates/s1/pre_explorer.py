

class PreProcess:
    def __init__(self, app):
        self.process_data(app)
        self.update_db(app)

    @classmethod
    def process_data(cls, app):
        print(app.name)

    @classmethod
    def update_db(cls, app):
        pass

