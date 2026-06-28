import argparse
import json
import os
from typing import List, Dict

class TaskManager:
    """
    A simple command-line task manager that persists data to a JSON file.
    """
    def __init__(self, filename: str = "tasks.json"):
        self.filename = filename
        self.tasks: List[Dict[str, str]] = []
        self.load_tasks()

    def load_tasks(self) -> None:
        """Loads tasks from the JSON file if it exists."""
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    self.tasks = json.load(f)
            except json.JSONDecodeError:
                print("Warning: Corrupt task file. Starting with an empty list.")
                self.tasks = []

    def save_tasks(self) -> None:
        """Saves the current list of tasks to the JSON file."""
        with open(self.filename, 'w') as f:
            json.dump(self.tasks, f, indent=4)

    def add_task(self, description: str) -> None:
        """Adds a new task to the list."""
        task = {
            "id": len(self.tasks) + 1,
            "description": description,
            "completed": False
        }
        self.tasks.append(task)
        self.save_tasks()
        print(f"Task added: '{description}'")

    def list_tasks(self) -> None:
        """Displays all tasks in the list."""
        if not self.tasks:
            print("No tasks found.")
            return

        print("\n--- Task List ---")
        for task in self.tasks:
            status = "[x]" if task["completed"] else "[ ]"
            print(f"{task['id']}: {status} {task['description']}")
        print("-----------------\n")

    def complete_task(self, task_id: int) -> None:
        """Marks a task as completed by ID."""
        for task in self.tasks:
            if task["id"] == task_id:
                task["completed"] = True
                self.save_tasks()
                print(f"Task {task_id} marked as completed.")
                return
        print(f"Error: Task {task_id} not found.")

    def delete_task(self, task_id: int) -> None:
        """Deletes a task by ID."""
        original_length = len(self.tasks)
        self.tasks = [t for t in self.tasks if t["id"] != task_id]
        
        if len(self.tasks) < original_length:
            self.save_tasks()
            print(f"Task {task_id} deleted.")
        else:
            print(f"Error: Task {task_id} not found.")

def main():
    parser = argparse.ArgumentParser(description="A simple CLI Task Manager.")
    parser.add_argument("command", choices=["add", "list", "complete", "delete"], help="The command to execute")
    parser.add_argument("args", nargs="*", help="Arguments for the command")
    parser.add_argument("-f", "--file", default="tasks.json", help="Specify a custom file for tasks")

    args = parser.parse_args()

    manager = TaskManager(args.file)

    if args.command == "add":
        if not args.args:
            print("Error: Please provide a task description.")
            return
        manager.add_task(" ".join(args.args))
    elif args.command == "list":
        manager.list_tasks()
    elif args.command == "complete":
        if not args.args:
            print("Error: Please provide a task ID.")
            return
        try:
            task_id = int(args.args[0])
            manager.complete_task(task_id)
        except ValueError:
            print("Error: Task ID must be an integer.")
    elif args.command == "delete":
        if not args.args:
            print("Error: Please provide a task ID.")
            return
        try:
            task_id = int(args.args[0])
            manager.delete_task(task_id)
        except ValueError:
            print("Error: Task ID must be an integer.")

if __name__ == "__main__":
    main()
