# app/controllers/employee_controller.py (Виправлена версія)

from flask import Blueprint, jsonify, request
from app.services.employee_service import EmployeeService 

employee_bp = Blueprint('employee', __name__, url_prefix='/api/employees')
employee_service = EmployeeService()


# ----------------------------------------
# I. EMPLOYEE CRUD ROUTES
# ----------------------------------------

@employee_bp.route('/', methods=['GET'])
def get_employees():
    employees = employee_service.get_all_employees()
    return jsonify(employees)

@employee_bp.route('/', methods=['POST'])
def create_employee():
    data = request.get_json()
    if not all(k in data for k in ('first_name', 'last_name', 'email', 'department_id')):
        return jsonify({'message': 'Missing required fields: name, email, department_id'}), 400
        
    new_employee = employee_service.create_employee(data)
    if new_employee:
        return jsonify(new_employee), 201 
    return jsonify({'message': 'Error creating employee'}), 500

@employee_bp.route('/<int:employee_id>', methods=['GET'])
def get_employee(employee_id):
    employee = employee_service.get_employee_by_id(employee_id)
    if employee:
        return jsonify(employee)
    return jsonify({'message': 'Employee not found'}), 404

@employee_bp.route('/<int:employee_id>', methods=['PUT'])
def update_employee(employee_id):
    data = request.get_json()
    if not employee_service.get_employee_by_id(employee_id):
         return jsonify({'message': 'Employee not found'}), 404
         
    if not all(k in data for k in ('first_name', 'last_name', 'email', 'department_id', 'is_it_staff')):
         return jsonify({'message': 'Missing all required update fields'}), 400
         
    updated_employee = employee_service.update_employee_data(employee_id, data)
    if updated_employee:
        return jsonify(updated_employee)
    return jsonify({'message': 'Update failed'}), 500

# app/controllers/employee_controller.py (ОНОВЛЕННЯ ЛОГІКИ DELETE)

@employee_bp.route('/<int:employee_id>', methods=['DELETE'])
def delete_employee(employee_id):
    deleted_result = employee_service.delete_employee_by_id(employee_id)
    
    # 1. Перевірка, чи DAO повернув рядок помилки (включаючи помилку Тригера)
    if isinstance(deleted_result, str):
        # Якщо це наш специфічний Тригер "Заборона видалення"
        if 'SQL Trigger Error: Видалення записів з таблиці employees заборонено!' in deleted_result:
             return jsonify({'message': f'Операція заборонена: {deleted_result.split("SQL Trigger Error:")[1].strip()}'}), 403 # 403 Forbidden
        # Інша помилка DB (якщо є)
        return jsonify({'message': f'Помилка бази даних: {deleted_result}'}), 500

    # 2. Якщо DAO повернув True/False (стандартна логіка)
    if deleted_result:
        return jsonify({'message': f'Employee with ID {employee_id} deleted successfully (WARNING: Should not have happened)'}), 204
    
    # 3. Якщо DAO повернув False (Employee не знайдено, або інший збій)
    employee_exists = employee_service.get_employee_by_id(employee_id)
    if employee_exists:
        # Це має бути помилка FK, яка не була перехоплена в DAO, але ми зберігаємо 409 для старої логіки
        return jsonify({'message': 'Deletion blocked (e.g., Foreign Key Constraint or unhandled error)'}), 409
        
    return jsonify({'message': 'Employee not found'}), 404


# ----------------------------------------
# II. REPORT ROUTES (Звіти)
# ----------------------------------------

@employee_bp.route('/departments/<int:department_id>', methods=['GET'])
def get_employees_by_department_route(department_id):
    employees = employee_service.get_employees_by_department_data(department_id)
    if employees:
        return jsonify(employees)
    return jsonify({'message': f'No employees found for Department ID {department_id}'}), 404

@employee_bp.route('/tickets/<int:ticket_id>/assignments', methods=['GET'])
def get_assignments_for_ticket_route(ticket_id):
    assignments = employee_service.get_assignments_for_ticket_data(ticket_id)
    if assignments:
        return jsonify(assignments)
    return jsonify({'message': f'No assignments found for Ticket ID {ticket_id}'}), 404

@employee_bp.route('/equipment_by_type_report', methods=['GET'])
def get_equipment_report_route():
    report = employee_service.get_equipment_report()
    if report:
        return jsonify(report)
    return jsonify({'message': 'Equipment report is empty'}), 200
    
# ----------------------------------------
# III. ЗАВДАННЯ ЛР №5
# ----------------------------------------

# 1. Тригер 1:M (ЗАЛИШАЄМО ЛИШЕ ОДИН ЕКЗЕМПЛЯР)
@employee_bp.route('/specializations/', methods=['POST'])
def create_specialization_route():
    data = request.get_json()
    if not 'name' in data:
        return jsonify({'message': 'Missing required fields: name'}), 400
        
    new_id_or_error = employee_service.create_specialization(data)
    
    # Перевірка, чи повернулося повідомлення про помилку (це рядок, а не int ID)
    if isinstance(new_id_or_error, str):
        # Обробка помилки SQLSTATE '45000' від тригера
        if 'SQL Trigger Error:' in new_id_or_error:
             return jsonify({'message': f'Помилка цілісності (Department ID): {new_id_or_error}'}), 409 # 409 Conflict
        return jsonify({'message': f'Помилка створення спеціалізації: {new_id_or_error}'}), 500
        
    if new_id_or_error:
        return jsonify({'message': 'Спеціалізацію успішно створено', 'id': new_id_or_error}), 201 
        
    return jsonify({'message': 'Невідома помилка при створенні спеціалізації'}), 500

# app/controllers/employee_controller.py (ДОДАТИ НОВИЙ МАРШРУТ)

@employee_bp.route('/equipment_types/', methods=['POST'])
def create_equipment_type_route():
    data = request.get_json()
    name = data.get('name')
    
    if not name:
        return jsonify({'message': 'Missing required field: name'}), 400
        
    new_id = employee_service.create_equipment_type(name)
    
    if new_id is not None:
        return jsonify({'message': f'Тип обладнання "{name}" успішно створено', 'id': new_id}), 201 
    
    # Якщо повертається None, це означає помилку БД або дублікат (як ми налаштували в ЗП)
    return jsonify({'message': 'Помилка створення типу обладнання (можливо, дублікат або збій БД)'}), 500

# app/controllers/employee_controller.py (ДОДАТИ НОВИЙ МАРШРУТ)

@employee_bp.route('/ticket_assignments/', methods=['POST'])
def assign_ticket_route():
    data = request.get_json()
    required_fields = ['assignee_first_name', 'assignee_last_name', 'ticket_title']
    
    if not all(k in data for k in required_fields):
        return jsonify({'message': 'Missing required fields: first_name, last_name, ticket_title'}), 400
        
    assignment_id = employee_service.assign_ticket(data)
    
    if assignment_id is not None:
        # Успіх
        return jsonify({
            'message': 'Призначення заявки успішно створено', 
            'assignment_id': assignment_id
        }), 201 
    
    # Якщо повертається None, це означає, що не знайдено працівника або заявку
    return jsonify({
        'message': 'Помилка призначення заявки: не знайдено виконавця або заявку з таким заголовком'
    }), 404

# app/controllers/employee_controller.py (ДОДАТИ НОВИЙ МАРШРУТ)

@employee_bp.route('/equipment_types/batch_insert', methods=['POST'])
def batch_insert_equipment_types_route():
    # Визначаємо стартовий ID. Поточні ID: 1, 2, 3. Починаємо з 4.
    start_id = 4 
    
    rows = employee_service.batch_insert_equipment_types(start_id)
    
    if rows is not None:
        return jsonify({
            'message': f'Успішно додано {rows} нових типів обладнання (Noname {start_id} до Noname {start_id + rows - 1})'
        }), 201
    
    return jsonify({'message': 'Помилка пакетного створення типів обладнання'}), 500

# app/controllers/employee_controller.py (ДОДАТИ НОВИЙ МАРШРУТ)

@employee_bp.route('/ticket_priority_stats', methods=['GET'])
def get_ticket_priority_stats_route():
    stats = employee_service.get_ticket_priority_stats()
    
    if stats:
        return jsonify(stats), 200
    
    return jsonify({'message': 'Помилка отримання статистики пріоритетів заявок'}), 500

# app/controllers/employee_controller.py (ДОДАТИ НОВИЙ МАРШРУТ)

@employee_bp.route('/equipment/split_log', methods=['POST'])
def split_equipment_log_route():
    result = employee_service.split_equipment_log()
    
    if result and result['rows_moved'] is not None:
        return jsonify({
            'message': f'Успішно розподілено {result["rows_moved"]} записів обладнання.',
            'createdTables': result['new_tables']
        }), 201
    
    return jsonify({'message': 'Помилка виконання процедури розподілу даних'}), 500

# app/controllers/employee_controller.py (ДОДАТИ НОВИЙ МАРШРУТ)

@employee_bp.route('/equipment_types/<int:type_id>', methods=['DELETE'])
def delete_equipment_type_route(type_id):
    deleted_result = employee_service.delete_equipment_type_by_id(type_id)
    
    # Перевірка на помилку тригера
    if isinstance(deleted_result, str):
        if 'SQL Trigger Error:' in deleted_result:
             # Помилка кардинальності або інша помилка тригера
             return jsonify({'message': f'Операція заборонена: {deleted_result.split("SQL Trigger Error:")[1].strip()}'}), 409 
        return jsonify({'message': f'Помилка бази даних: {deleted_result}'}), 500

    if deleted_result:
        return jsonify({'message': f'Equipment Type ID {type_id} успішно видалено'}), 204
    
    return jsonify({'message': 'Equipment Type не знайдено'}), 404

# app/controllers/employee_controller.py (ДОДАТИ НОВИЙ МАРШРУТ)

@employee_bp.route('/equipment_types/logs', methods=['GET'])
def get_equipment_type_logs_route():
    logs = employee_service.get_equipment_type_deletion_logs()
    
    if logs is not None:
        return jsonify(logs), 200
    
    return jsonify({'message': 'Помилка отримання логів видалення'}), 500

