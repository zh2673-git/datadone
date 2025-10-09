#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 注意：此文件是cli_interface.py的一部分，包含分组管理相关方法

def manage_groups_menu(self):
    """
    分组管理菜单
    """
    options = [
        "创建新分组",
        "编辑分组",
        "删除分组",
        "查看分组",
        "导入分组",
        "导出分组"
    ]
    
    while True:
        choice = self.display_menu(options, "分组管理")
        
        if choice == -1:  # 返回
            break
        
        if choice == 0:  # 创建新分组
            self.create_group()
        elif choice == 1:  # 编辑分组
            self.edit_group()
        elif choice == 2:  # 删除分组
            self.delete_group()
        elif choice == 3:  # 查看分组
            self.view_groups()
        elif choice == 4:  # 导入分组
            self.import_groups()
        elif choice == 5:  # 导出分组
            self.export_groups()

def create_group(self):
    """
    创建新分组
    """
    print("\n创建新分组")
    print("-" * 15)
    
    # 初始化分组管理器（如果尚未创建）
    if self.group_manager is None:
        from src.utils.group import GroupManager
        self.group_manager = GroupManager()
    
    # 获取分组名称
    group_name = self.get_input("请输入分组名称")
    
    # 检查是否已存在同名分组
    if self.group_manager.get_group(group_name):
        if not self.confirm(f"已存在名为 '{group_name}' 的分组，是否覆盖？"):
            return
    
    # 选择成员
    members = []
    
    # 从已加载的数据中获取人员列表
    all_persons = set()
    for data_type, model in self.data_models.items():
        if model:
            all_persons.update(model.get_persons())
    
    if not all_persons:
        self.display_error("未找到任何人员，请先加载数据")
        return
    
    # 将人员列表转为列表以便排序
    person_list = sorted(list(all_persons))
    
    print("\n选择分组成员:")
    
    while True:
        # 显示当前已选成员
        if members:
            print(f"\n当前已选成员: {', '.join(members)}")
        
        # 选择成员
        indices = self.get_multiple_choice(person_list, prompt="请选择成员（可多选）", multiple=True)
        
        for idx in indices:
            if person_list[idx] not in members:
                members.append(person_list[idx])
        
        if not self.confirm("继续添加成员？"):
            break
    
    # 添加分组
    if members:
        success = self.group_manager.add_group(group_name, members)
        if success:
            self.display_success(f"成功创建分组 '{group_name}'，包含 {len(members)} 个成员")
            
            # 更新所有分析器的分组管理器
            self.update_analyzers_group_manager()
        else:
            self.display_error(f"创建分组 '{group_name}' 失败")
    else:
        self.display_error("未选择任何成员，分组创建失败")

def edit_group(self):
    """
    编辑分组
    """
    print("\n编辑分组")
    print("-" * 15)
    
    if not self.group_manager or not self.group_manager.groups:
        self.display_error("没有可编辑的分组")
        return
    
    # 选择要编辑的分组
    group_names = self.group_manager.get_group_names()
    idx = self.get_multiple_choice(group_names, prompt="请选择要编辑的分组", multiple=False)
    group_name = group_names[idx]
    
    # 获取当前分组成员
    current_members = self.group_manager.get_group(group_name)
    
    print(f"\n分组 '{group_name}' 当前成员: {', '.join(current_members)}")
    
    # 选择编辑操作
    edit_options = [
        "添加成员",
        "移除成员",
        "重命名分组"
    ]
    
    edit_choice = self.get_multiple_choice(edit_options, prompt="请选择编辑操作", multiple=False)
    
    if edit_choice == 0:  # 添加成员
        # 从已加载的数据中获取人员列表
        all_persons = set()
        for data_type, model in self.data_models.items():
            if model:
                all_persons.update(model.get_persons())
        
        # 排除已在分组中的成员
        available_persons = sorted(list(all_persons - set(current_members)))
        
        if not available_persons:
            self.display_error("没有可添加的成员")
            return
        
        # 选择要添加的成员
        indices = self.get_multiple_choice(available_persons, prompt="请选择要添加的成员（可多选）", multiple=True)
        
        new_members = [available_persons[idx] for idx in indices]
        
        if new_members:
            # 添加新成员
            updated_members = current_members + new_members
            success = self.group_manager.update_group(group_name, updated_members)
            
            if success:
                self.display_success(f"成功添加 {len(new_members)} 个成员到分组 '{group_name}'")
            else:
                self.display_error(f"添加成员到分组 '{group_name}' 失败")
        else:
            self.display_error("未选择任何成员")
    
    elif edit_choice == 1:  # 移除成员
        if not current_members:
            self.display_error("分组中没有成员可移除")
            return
        
        # 选择要移除的成员
        indices = self.get_multiple_choice(current_members, prompt="请选择要移除的成员（可多选）", multiple=True)
        
        to_remove = [current_members[idx] for idx in indices]
        
        if to_remove:
            # 移除成员
            updated_members = [m for m in current_members if m not in to_remove]
            
            if not updated_members:
                self.display_error("移除后分组将不包含任何成员，请至少保留一个成员")
                return
            
            success = self.group_manager.update_group(group_name, updated_members)
            
            if success:
                self.display_success(f"成功从分组 '{group_name}' 移除 {len(to_remove)} 个成员")
            else:
                self.display_error(f"从分组 '{group_name}' 移除成员失败")
        else:
            self.display_error("未选择任何成员")
    
    elif edit_choice == 2:  # 重命名分组
        new_name = self.get_input("请输入新的分组名称")
        
        if not new_name:
            self.display_error("分组名称不能为空")
            return
        
        if new_name == group_name:
            self.display_error("新名称与当前名称相同")
            return
        
        # 检查是否已存在同名分组
        if self.group_manager.get_group(new_name):
            if not self.confirm(f"已存在名为 '{new_name}' 的分组，是否覆盖？"):
                return
        
        # 重命名分组
        success = self.group_manager.rename_group(group_name, new_name)
        
        if success:
            self.display_success(f"成功将分组 '{group_name}' 重命名为 '{new_name}'")
        else:
            self.display_error(f"重命名分组 '{group_name}' 失败")

def delete_group(self):
    """
    删除分组
    """
    print("\n删除分组")
    print("-" * 15)
    
    if not self.group_manager or not self.group_manager.groups:
        self.display_error("没有可删除的分组")
        return
    
    # 选择要删除的分组
    group_names = self.group_manager.get_group_names()
    idx = self.get_multiple_choice(group_names, prompt="请选择要删除的分组", multiple=False)
    group_name = group_names[idx]
    
    # 确认删除
    if self.confirm(f"确认删除分组 '{group_name}'？"):
        success = self.group_manager.remove_group(group_name)
        
        if success:
            self.display_success(f"成功删除分组 '{group_name}'")
        else:
            self.display_error(f"删除分组 '{group_name}' 失败")

def view_groups(self):
    """
    查看分组
    """
    print("\n查看分组")
    print("-" * 15)
    
    if not self.group_manager or not self.group_manager.groups:
        self.display_error("没有分组可查看")
        return
    
    # 显示所有分组
    group_names = self.group_manager.get_group_names()
    print(f"共有 {len(group_names)} 个分组：")
    
    for i, name in enumerate(group_names, 1):
        members = self.group_manager.get_group(name)
        print(f"\n{i}. {name} ({len(members)} 个成员)")
        
        # 显示分组成员
        for j, member in enumerate(members, 1):
            print(f"   {j}. {member}")

def import_groups(self):
    """
    导入分组
    """
    print("\n导入分组")
    print("-" * 15)
    
    # 获取分组文件路径
    file_path = self.get_path_input("请输入分组文件路径(.json)", must_exist=True, is_dir=False)
    
    if not file_path:
        return
    
    # 初始化分组管理器（如果尚未创建）
    if self.group_manager is None:
        from src.utils.group import GroupManager
        self.group_manager = GroupManager()
    
    try:
        # 导入分组
        success = self.group_manager.load_from_file(file_path)
        
        if success:
            self.display_success(f"成功导入分组，共 {len(self.group_manager.groups)} 个分组")
            
            # 更新所有分析器的分组管理器
            self.update_analyzers_group_manager()
        else:
            self.display_error("导入分组失败")
    
    except Exception as e:
        self.display_error(f"导入分组时出错: {str(e)}")

def export_groups(self):
    """
    导出分组
    """
    print("\n导出分组")
    print("-" * 15)
    
    if not self.group_manager or not self.group_manager.groups:
        self.display_error("没有分组可导出")
        return
    
    # 获取导出文件路径
    file_path = self.get_input("请输入导出文件路径(.json)")
    
    if not file_path:
        return
    
    # 确保文件扩展名为.json
    if not file_path.endswith('.json'):
        file_path += '.json'
    
    try:
        # 导出分组
        success = self.group_manager.save_to_file(file_path)
        
        if success:
            self.display_success(f"成功导出分组到 {file_path}")
        else:
            self.display_error("导出分组失败")
    
    except Exception as e:
        self.display_error(f"导出分组时出错: {str(e)}")

def update_analyzers_group_manager(self):
    """
    更新所有分析器的分组管理器
    """
    for analyzer_type, analyzer in self.analyzers.items():
        analyzer.group_manager = self.group_manager