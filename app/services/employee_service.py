# app/services/employee_service.py

from app.dao.employee_dao import EmployeeDAO 

class EmployeeService:
    def __init__(self):
        self.dao = EmployeeDAO()

    # ----------------------------------------
    # I. DTO та Трансформація
    # ----------------------------------------

    def _to_employee_dto(self, employee_data):
        if not employee_data:
            return None
        return {
            'id': employee_data.get('employee_id'),
            'firstName': employee_data.get('first_name'),
            'lastName': employee_data.get('last_name'),
            'email': employee_data.get('email'),
            'departmentId': employee_data.get('department_id'),
            'isItStaff': bool(employee_data.get('is_it_staff'))
        }

    # ----------------------------------------
    # II. EMPLOYEE CRUD ЛОГІКА
    # ----------------------------------------
    
    def get_all_employees(self):
        employees = self.dao.get_all_employees()
        return [self._to_employee_dto(e) for e in employees]
    
    def get_employee_by_id(self, employee_id):
        employee = self.dao.get_employee_by_id(employee_id)
        return self._to_employee_dto(employee)

    def create_employee(self, data):
        new_id = self.dao.create_employee(data)
        if new_id:
            return self.get_employee_by_id(new_id)
        return None

    def update_employee_data(self, employee_id, data):
        updated = self.dao.update_employee(employee_id, data)
        if updated:
            return self.get_employee_by_id(employee_id)
        return None
    
    def delete_employee_by_id(self, employee_id):
        return self.dao.delete_employee(employee_id)

    # ----------------------------------------
    # III. ЛОГІКА ГРУПУВАННЯ ТА ЗВІТІВ
    # ----------------------------------------

    # 1. M:1 та M:M звіти
    def get_employees_by_department_data(self, department_id):
        return self.dao.get_employees_by_department(department_id)

    def get_assignments_for_ticket_data(self, ticket_id):
        return self.dao.get_assignments_for_ticket(ticket_id)

    # 2. Групування Звіту за Типом Обладнання
    def _group_equipment_by_type(self, flat_report_list):
        result_list = []
        for item in flat_report_list:
            type_name = item.get('type_name')
            models_list = [m.strip() for m in item.get('models_used', '').split(',') if m.strip()]
            
            result_list.append({
                'equipmentType': type_name,
                'totalCount': int(item.get('total_count', 0)),
                'inUseCount': int(item.get('in_use_count', 0)),
                'models': models_list
            })
        return result_list

    def get_equipment_report(self):
        flat_report = self.dao.get_equipment_count_by_type()
        return self._group_equipment_by_type(flat_report)
        
    # ----------------------------------------
    # IV. ЗАВДАННЯ ЛР №5
    # ----------------------------------------

    # 1. Тригер 1:M
    def create_specialization(self, data):
        return self.dao.create_specialization(data)

# app/services/employee_service.py (ДОДАТИ всередині класу EmployeeService)

    # 2.a. SP
    def create_equipment_type(self, name):
        return self.dao.create_equipment_type_sp(name)

# app/services/employee_service.py (ДОДАТИ всередині класу EmployeeService)

    # 2.b. SP
    def assign_ticket(self, data):
        return self.dao.assign_ticket_sp(
            data.get('assignee_first_name'),
            data.get('assignee_last_name'),
            data.get('ticket_title'),
            data.get('role', 'resolver') # За замовчуванням 'resolver'
        )

# app/services/employee_service.py (ДОДАТИ всередині класу EmployeeService)

    # 2.c. SP
    def batch_insert_equipment_types(self, start_id):
        # Передаємо стартовий ID. Ми можемо його жорстко задати тут, або прийняти від контролера.
        # Для простоти передамо 4, оскільки поточний максимальний ID - 3.
        return self.dao.batch_insert_equipment_types_sp(start_id)

# app/services/employee_service.py (ДОДАТИ всередині класу EmployeeService)

    # 2.d. UDF + SP
    def get_ticket_priority_stats(self):
        stats = self.dao.get_ticket_priority_stats_sp()
        if stats:
            # Конвертуємо лише числові поля у float для JSON-серіалізації
            clean_stats = {
                'columnName': stats.get('column_name'),
                'tableName': stats.get('table_name'),
                'maxPriority': float(stats['max_priority']),
                'minPriority': float(stats['min_priority']),
                'sumPriority': float(stats['sum_priority']),
                'avgPriority': float(stats['avg_priority']),
            }
            return clean_stats
        return None

# app/services/employee_service.py (ДОДАТИ всередині класу EmployeeService)

    # 2.e.i. SP з курсором
    def split_equipment_log(self):
        return self.dao.split_equipment_log_sp()

# app/services/employee_service.py (ДОДАТИ всередині класу EmployeeService)

    # 3.b. Кардинальність
    def delete_equipment_type_by_id(self, type_id):
        return self.dao.delete_equipment_type(type_id)

# app/services/employee_service.py (ДОДАТИ всередині класу EmployeeService)

    # 3.c. Логування
    def get_equipment_type_deletion_logs(self):
        return self.dao.get_equipment_type_deletion_logs()