import time
import pandas as pd
from difflib import SequenceMatcher
from typing import List, Dict, Any, Optional, Tuple
from tqdm import tqdm

from ..api.api_client import YMGalAPIClient
from ..data.data_processor import DataProcessor


class MatchingEngine:
    """匹配引擎，负责产品匹配逻辑"""
    
    def __init__(self, api_client: YMGalAPIClient, data_processor: DataProcessor):
        self.api_client = api_client
        self.data_processor = data_processor
    
    def calculate_similarity(self, str1: str, str2: str) -> float:
        """利用 ``difflib.SequenceMatcher`` 计算字符串相似度。"""
        return SequenceMatcher(None, str1.lower(), str2.lower()).ratio()
    
    def match_bgm_products_and_save(
        self,
        input_file: str = "bgm_archive_20250525 (1).xlsx",
        output_file: str = "products_matched.xlsx",
        unmatched_file: str = "products_unmatched.xlsx",
        org_output_file: str = "organizations_info.xlsx"
    ) -> None:
        """
        读取原始数据Excel -> 目标平台搜索匹配 -> 写结果
        支持 **断点续跑** ：已处理过的原始名称会跳过。
        """
        # 1. 读取原始源文件
        df_bgm = self.data_processor.read_bgm_data(input_file)
        
        product_names_cn: List[str] = df_bgm["中文名"].dropna().astype(str).tolist()
        
        # 2. 加载已处理过的 ID (用于断点续跑)
        processed_ids = self.data_processor.get_processed_ids(output_file)

        # 3. 初始化输出文件 & token
        if not self.api_client.initialize_token():
            print("无法获取 token，流程终止")
            return

        self.data_processor.init_excel(output_file)
        self.data_processor.init_org_excel(org_output_file)

        # 4. 加载已有公司信息到内存，避免重复查询
        processed_orgs = self.data_processor.get_processed_orgs(org_output_file)

        # 5. 遍历原始数据行并匹配
        for idx, row in tqdm(df_bgm.iterrows(), total=len(df_bgm), desc="处理产品"):
            bgm_id = str(row['id']) if 'id' in row and pd.notna(row['id']) else f"ROW_{idx}"
            
            if bgm_id in processed_ids:
                continue

            jp_name = str(row["日文名"]).strip() if pd.notna(row["日文名"]) else ""
            cn_name = str(row["中文名"]).strip() if pd.notna(row["中文名"]) else ""

            if not jp_name and not cn_name:
                self.data_processor.append_unmatched_to_excel(f"ID_{bgm_id}_空名称", unmatched_file)
                continue

            best_match = None
            best_score = -1.0  # 初始化最高得分
            match_source = ""

            # 尝试匹配日文名
            if jp_name:
                jp_matches = self.api_client.search_ym_top_matches(jp_name)
                if jp_matches and jp_matches[0]["score"] > best_score:
                    best_match = jp_matches[0]
                    best_score = best_match["score"]
                    match_source = "日文名"

            # 尝试匹配中文名
            if cn_name:
                cn_matches = self.api_client.search_ym_top_matches(cn_name)
                if cn_matches and cn_matches[0]["score"] > best_score:
                    best_match = cn_matches[0]
                    best_score = best_match["score"]
                    match_source = "中文名"

            if best_match:
                row_list: List[Dict[str, Any]] = []
                # ---- 公司信息处理 ----------------------------------------
                org_id = str(best_match.get("orgId", ""))
                org_info = None  # type: Optional[Dict[str, Any]]

                if org_id:
                    should_retry = False
                    if org_id in processed_orgs:
                        # 信息不完整时重试 (最多 3 次)
                        existing = processed_orgs[org_id]["info"]
                        if not existing.get("website") or not existing.get("description"):
                            should_retry = True
                            processed_orgs[org_id]["retry_count"] += 1
                    else:
                        should_retry = True
                        processed_orgs[org_id] = {"info": {}, "retry_count": 1}

                    if should_retry and processed_orgs[org_id]["retry_count"] <= 3:
                        org_info = self.api_client.get_organization_details(org_id)
                        if org_info:
                            processed_orgs[org_id]["info"] = org_info
                            self.data_processor.append_org_to_excel(org_info, org_output_file)
                    else:
                        org_info = processed_orgs[org_id]["info"]

                # ---- 组装行数据 -----------------------------------------
                row_data = {
                    "bgm_id": bgm_id,
                    "bgm产品": jp_name if jp_name else cn_name, # 使用非空的原始名称作为bgm产品
                    "name": best_match["name"],
                    "chineseName": best_match["chineseName"],
                    "ym_id": best_match["ym_id"],
                    "score": best_match["score"],
                    "orgId": org_id,
                    "orgName": (org_info or {}).get("name", best_match.get("orgName", "")),
                    "orgWebsite": (org_info or {}).get("website", best_match.get("orgWebsite", "")),
                    "orgDescription": (org_info or {}).get("description", best_match.get("orgDescription", "")),
                    "匹配来源": match_source
                }
                row_list.append(row_data)

                self.data_processor.append_to_excel(row_list, output_file)
            else:
                self.data_processor.append_unmatched_to_excel(f"ID_{bgm_id}_未匹配", unmatched_file)

            # 避免触发接口限流
            time.sleep(0.05)

        print("\n所有匹配结果已保存。🎉")
    
    def match_bgm_products_with_aliases_and_save(
        self,
        input_file: str = "主表_updated_processed_aliases_20250621_124012.xlsx",
        output_file: str = "products_matched.xlsx",
        unmatched_file: str = "products_unmatched.xlsx",
        org_output_file: str = "organizations_info.xlsx"
    ) -> None:
        """
        读取包含别名的原始数据Excel -> 目标平台搜索匹配 -> 写结果
        支持 **断点续跑** ：已处理过的原始名称会跳过。
        """
        # 1. 读取原始源文件
        df_bgm = self.data_processor.read_bgm_data_with_aliases(input_file)

        # 2. 加载已处理过的 ID (用于断点续跑)
        processed_ids = self.data_processor.get_processed_ids(output_file)

        # 3. 初始化输出文件 & token
        if not self.api_client.initialize_token():
            print("无法获取 token，流程终止")
            return

        self.data_processor.init_excel(output_file)
        self.data_processor.init_org_excel(org_output_file)

        # 4. 加载已有公司信息到内存，避免重复查询
        processed_orgs = self.data_processor.get_processed_orgs(org_output_file)

        # 5. 遍历原始数据行并匹配
        for idx, row in tqdm(df_bgm.iterrows(), total=len(df_bgm), desc="处理产品"):
            bgm_id = str(row['bgm_id']) if 'bgm_id' in row and pd.notna(row['bgm_id']) else f"ROW_{idx}"

            if bgm_id in processed_ids:
                continue

            # 只用别名列进行匹配
            alias_cols = [col for col in row.index if col.startswith("别名")]
            aliases = [str(row[col]).strip() for col in alias_cols if pd.notna(row[col]) and str(row[col]).strip()]

            # 1. 获取原始分数，并确保为浮点数，默认0
            original_score = 0.0
            if 'score' in row and pd.notna(row['score']):
                try:
                    original_score = float(row['score'])
                except (ValueError, TypeError):
                    pass # 如果转换失败，则保持0.0

            # 2. 查找所有别名中的最佳匹配
            best_match = None
            best_score = -1.0  # 初始化别名匹配的最高分
            match_source = ""

            for i, alias in enumerate(aliases):
                matches = self.api_client.search_ym_top_matches(alias)
                if matches and matches[0]["score"] > best_score:
                    best_match = matches[0]
                    best_score = best_match["score"]
                    match_source = f"别名{i+1}"

            if best_match and best_score > original_score:
                row_list: List[Dict[str, Any]] = []
                # ---- 公司信息处理 (仅当别名更优时才查询) ----
                org_id = str(best_match.get("orgId", ""))
                org_info = None  # type: Optional[Dict[str, Any]]

                if org_id:
                    should_retry = False
                    if org_id in processed_orgs:
                        existing = processed_orgs[org_id]["info"]
                        if not existing.get("website") or not existing.get("description"):
                            should_retry = True
                            processed_orgs[org_id]["retry_count"] += 1
                    else:
                        should_retry = True
                        processed_orgs[org_id] = {"info": {}, "retry_count": 1}

                    if should_retry and processed_orgs[org_id]["retry_count"] <= 3:
                        org_info = self.api_client.get_organization_details(org_id)
                        if org_info:
                            processed_orgs[org_id]["info"] = org_info
                            self.data_processor.append_org_to_excel(org_info, org_output_file)
                    else:
                        org_info = processed_orgs[org_id]["info"]

                # ---- 组装新行数据 ----
                row_data = {
                    "bgm_id": bgm_id,
                    "bgm产品": row.get('bgm产品') or row.get('原始bgm产品名称'),
                    "name": best_match["name"],
                    "chineseName": best_match["chineseName"],
                    "ym_id": best_match["ym_id"],
                    "score": best_score,
                    "orgId": org_id,
                    "orgName": (org_info or {}).get("name", best_match.get("orgName", "")),
                    "orgWebsite": (org_info or {}).get("website", best_match.get("orgWebsite", "")),
                    "orgDescription": (org_info or {}).get("description", best_match.get("orgDescription", "")),
                    "匹配来源": match_source
                }
                row_list.append(row_data)
                self.data_processor.append_to_excel(row_list, output_file)
            else:
                # ---- 组装原始行数据 ----
                row_data = {
                    "bgm_id": bgm_id,
                    "bgm产品": row.get('bgm产品') or row.get('原始bgm产品名称'),
                    "name": row.get('name'),
                    "chineseName": row.get('chineseName'),
                    "ym_id": row.get('ym_id'),
                    "score": original_score,
                    "orgId": row.get('orgId'),
                    "orgName": row.get('orgName'),
                    "orgWebsite": row.get('orgWebsite'),
                    "orgDescription": row.get('orgDescription'),
                    "匹配来源": "原始"
                }
                row_list = [row_data]
                self.data_processor.append_to_excel(row_list, output_file)

            # 避免触发接口限流
            time.sleep(0.001)

        print("\n所有匹配结果已保存。🎉")
    
    def match_target_with_source(
        self,
        target_file: str = "products_matched.xlsx",
        source_file: str = "processed_products_test5.xlsx",
        output_file: str = "target_source_matched.csv"
    ) -> None:
        """
        按名称相似度将 **目标平台产品** 与 **原始产品** 对齐，并输出 CSV 文件。
        """
        print("开始匹配目标平台产品与原始产品…")

        # 1. 读取两侧数据
        target_df = pd.read_excel(target_file)
        source_df = pd.read_excel(source_file)

        results = []

        # 2. 遍历目标平台条目
        for _, target_row in target_df.iterrows():
            target_name = target_row["name"]
            target_cn_name = target_row["chineseName"]
            target_id = target_row["ym_id"]

            best_match, best_score = None, 0.0
            for _, source_row in source_df.iterrows():
                score = self.calculate_similarity(target_name, source_row["产品名称"])
                if score > best_score:
                    best_match, best_score = source_row, score

            if best_match is not None and best_score >= 0.8:
                results.append({
                    "target_id": target_id,
                    "target_name": target_name,
                    "target_chinese_name": target_cn_name,
                    "source_id": best_match.get("产品ID", ""),
                    "source_name": best_match["产品名称"],
                    "source_score": best_match.get("评分", ""),
                    "source_rank": best_match.get("排名", ""),
                    "source_votes": best_match.get("投票数", ""),
                    "source_summary": best_match.get("简介", ""),
                    "match_score": round(best_score, 4)
                })
                print(f"匹配成功：{target_name} -> {best_match['产品名称']} (得分: {best_score:.4f})")

        pd.DataFrame(results).to_csv(output_file, index=False, encoding="utf-8-sig")
        print(f"\n匹配结果已保存到：{output_file}  (共 {len(results)} 条)")