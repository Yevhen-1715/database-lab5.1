from flask import Flask
from flask_cors import CORS 
from app.config import Config 


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config) 
    CORS(app) 

    from app.controllers.employee_controller import employee_bp
    app.register_blueprint(employee_bp)
    
    return app