#!/bin/bash
# 表行数对比脚本包装器

# 设置默认参数
DATA_DT=$(date +%Y%m%d -d "yesterday")  # 默认为昨天的日期
OUTPUT_DIR="/home/lkw/qianyi-project/new/output"
SKIP_HIVE=false

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        --data-dt)
            DATA_DT="$2"
            shift 2
            ;;
        --output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --skip-hive)
            SKIP_HIVE=true
            shift
            ;;
        --help)
            echo "用法: $0 [选项]"
            echo "选项:"
            echo "  --data-dt DATE    分区日期 (默认: 昨天)"
            echo "  --output-dir DIR  输出目录"
            echo "  --skip-hive       跳过Hive加载"
            echo "  --help            显示帮助信息"
            exit 0
            ;;
        *)
            echo "错误: 未知选项 $1"
            echo "使用 --help 查看帮助"
            exit 1
            ;;
    esac
done

# 检查Python脚本是否存在
SCRIPT_DIR="$(dirname "$0")"
SCRIPT_NAME="compare_num_rows.py"
SCRIPT_PATH="$SCRIPT_DIR/$SCRIPT_NAME"

if [ ! -f "$SCRIPT_PATH" ]; then
    echo "错误: Python脚本不存在: $SCRIPT_PATH"
    exit 1
fi

# 创建输出目录
mkdir -p "$OUTPUT_DIR"

echo "开始表行数对比..."
echo "数据日期: $DATA_DT"
echo "输出目录: $OUTPUT_DIR"

# 构建命令参数
CMD="python $SCRIPT_PATH --data-dt $DATA_DT --output-dir $OUTPUT_DIR"

if [ "$SKIP_HIVE" = true ]; then
    CMD="$CMD --skip-hive"
fi

echo "执行命令: $CMD"

# 执行Python脚本
$CMD

if [ $? -eq 0 ]; then
    echo "对比完成!"

    # 显示生成的CSV文件
    TODAY=$(date +%Y%m%d)
    CSV_FILE="$OUTPUT_DIR/$TODAY/${DATA_DT}_table_comparison.csv"

    if [ -f "$CSV_FILE" ]; then
        echo "生成的CSV文件: $CSV_FILE"

        # 显示CSV文件行数
        CSV_LINES=$(wc -l < "$CSV_FILE")
        echo "CSV文件行数: $((CSV_LINES - 1)) (不含表头)"

        # 显示前几行内容
        echo -e "\nCSV文件前5行:"
        head -n 6 "$CSV_FILE"
    else
        echo "警告: CSV文件未生成"
    fi
else
    echo "错误: 对比过程失败"
    exit 1
fi