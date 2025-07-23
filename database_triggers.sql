-- 数据库触发器实现（MySQL 示例）
-- 用于捕获数据变更并推送到同步队列

-- 创建同步队列表
CREATE TABLE IF NOT EXISTS sync_queue (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    record_id VARCHAR(100) NOT NULL,
    action VARCHAR(20) NOT NULL, -- INSERT, UPDATE, DELETE
    old_data JSON,
    new_data JSON,
    sync_hash VARCHAR(64),
    sync_source VARCHAR(20) DEFAULT 'database',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP NULL,
    status VARCHAR(20) DEFAULT 'pending',
    retry_count INT DEFAULT 0,
    error_message TEXT,
    INDEX idx_status_created (status, created_at),
    INDEX idx_table_record (table_name, record_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建同步日志表
CREATE TABLE IF NOT EXISTS sync_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    sync_id VARCHAR(100) UNIQUE,
    table_name VARCHAR(100),
    record_id VARCHAR(100),
    direction VARCHAR(50), -- feishu_to_db or db_to_feishu
    sync_hash VARCHAR(64),
    status VARCHAR(20),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_sync_hash (sync_hash),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 示例：为 users 表创建触发器
DELIMITER $$

-- INSERT 触发器
CREATE TRIGGER users_after_insert
AFTER INSERT ON users
FOR EACH ROW
BEGIN
    DECLARE sync_data JSON;
    DECLARE data_hash VARCHAR(64);
    
    -- 构建数据 JSON
    SET sync_data = JSON_OBJECT(
        'id', NEW.id,
        'name', NEW.name,
        'age', NEW.age,
        'email', NEW.email,
        'created_at', NEW.created_at,
        'updated_at', NEW.updated_at
    );
    
    -- 计算数据哈希
    SET data_hash = MD5(sync_data);
    
    -- 检查是否需要同步（避免循环）
    IF NOT EXISTS (
        SELECT 1 FROM sync_log 
        WHERE sync_hash = data_hash 
        AND created_at > DATE_SUB(NOW(), INTERVAL 10 SECOND)
    ) THEN
        -- 插入同步队列
        INSERT INTO sync_queue (
            table_name, 
            record_id, 
            action, 
            new_data, 
            sync_hash
        ) VALUES (
            'users',
            NEW.id,
            'INSERT',
            sync_data,
            data_hash
        );
    END IF;
END$$

-- UPDATE 触发器
CREATE TRIGGER users_after_update
AFTER UPDATE ON users
FOR EACH ROW
BEGIN
    DECLARE old_data JSON;
    DECLARE new_data JSON;
    DECLARE data_hash VARCHAR(64);
    
    -- 构建旧数据 JSON
    SET old_data = JSON_OBJECT(
        'id', OLD.id,
        'name', OLD.name,
        'age', OLD.age,
        'email', OLD.email,
        'updated_at', OLD.updated_at
    );
    
    -- 构建新数据 JSON
    SET new_data = JSON_OBJECT(
        'id', NEW.id,
        'name', NEW.name,
        'age', NEW.age,
        'email', NEW.email,
        'updated_at', NEW.updated_at
    );
    
    -- 计算数据哈希
    SET data_hash = MD5(new_data);
    
    -- 检查是否需要同步
    IF NOT EXISTS (
        SELECT 1 FROM sync_log 
        WHERE sync_hash = data_hash 
        AND created_at > DATE_SUB(NOW(), INTERVAL 10 SECOND)
    ) THEN
        -- 插入同步队列
        INSERT INTO sync_queue (
            table_name, 
            record_id, 
            action, 
            old_data,
            new_data, 
            sync_hash
        ) VALUES (
            'users',
            NEW.id,
            'UPDATE',
            old_data,
            new_data,
            data_hash
        );
    END IF;
END$$

-- DELETE 触发器
CREATE TRIGGER users_after_delete
AFTER DELETE ON users
FOR EACH ROW
BEGIN
    DECLARE old_data JSON;
    DECLARE data_hash VARCHAR(64);
    
    -- 构建旧数据 JSON
    SET old_data = JSON_OBJECT(
        'id', OLD.id,
        'name', OLD.name,
        'age', OLD.age,
        'email', OLD.email
    );
    
    -- 计算数据哈希
    SET data_hash = MD5(CONCAT('DELETE_', OLD.id));
    
    -- 插入同步队列
    INSERT INTO sync_queue (
        table_name, 
        record_id, 
        action, 
        old_data,
        sync_hash
    ) VALUES (
        'users',
        OLD.id,
        'DELETE',
        old_data,
        data_hash
    );
END$$

DELIMITER ;

-- 存储过程：处理同步队列
DELIMITER $$

CREATE PROCEDURE process_sync_queue()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE v_id BIGINT;
    DECLARE v_table_name VARCHAR(100);
    DECLARE v_record_id VARCHAR(100);
    DECLARE v_action VARCHAR(20);
    DECLARE v_new_data JSON;
    DECLARE v_sync_hash VARCHAR(64);
    
    -- 声明游标
    DECLARE cur CURSOR FOR 
        SELECT id, table_name, record_id, action, new_data, sync_hash
        FROM sync_queue 
        WHERE status = 'pending' 
        AND retry_count < 3
        ORDER BY created_at ASC 
        LIMIT 100;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    
    OPEN cur;
    
    read_loop: LOOP
        FETCH cur INTO v_id, v_table_name, v_record_id, v_action, v_new_data, v_sync_hash;
        
        IF done THEN
            LEAVE read_loop;
        END IF;
        
        -- 记录同步日志
        INSERT INTO sync_log (
            sync_id,
            table_name,
            record_id,
            direction,
            sync_hash,
            status
        ) VALUES (
            CONCAT(v_table_name, '_', v_record_id, '_', v_sync_hash),
            v_table_name,
            v_record_id,
            'db_to_feishu',
            v_sync_hash,
            'processing'
        ) ON DUPLICATE KEY UPDATE status = 'processing';
        
        -- 标记为处理中
        UPDATE sync_queue 
        SET status = 'processing', 
            processed_at = NOW()
        WHERE id = v_id;
        
    END LOOP;
    
    CLOSE cur;
END$$

DELIMITER ;

-- 创建定时事件，每秒执行一次队列处理
CREATE EVENT IF NOT EXISTS process_sync_queue_event
ON SCHEDULE EVERY 1 SECOND
DO CALL process_sync_queue();