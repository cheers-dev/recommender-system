import psycopg2
import numpy as np
import pandas as pd
from scipy.spatial import distance

column_mapping = {
    "american": "美式",
    "bar": "酒吧",
    "chinese": "中式",
    "dessert": "甜點",
    "exotic": "異國料理",
    "french": "法式",
    "hongkong": "港式",
    "italian": "義式",
    "japanese": "日式",
    "korean": "韓式",
    "southeastAsian": "東南亞",
    "thai": "泰式",
    "vietnamese": "越式",
    "western": "西式",
}

class FindLeader:
    def __init__(self, db_config):
        self.conn = psycopg2.connect(**db_config)
    
    def __del__(self):
        if self.conn:
            self.conn.close()

    def get_user_preference(self, chatroom_id):
        """
        Get group members' preference by join two table.

        Returns:
            rows (DataFrame): group members' preference
        """
        cur = self.conn.cursor()

        sql = f"""
        SELECT cp.user_id, up.american, up.bar, up.chinese, up.dessert, up.exotic,
            up.french, up.hongkong, up.italian, up.japanese, up.korean,
            up."southeastAsian", up.thai, up.vietnamese, up.western
        FROM chatroom_participant cp
        JOIN user_preference up ON cp.user_id = up.user_id
        WHERE cp.chatroom_id = '{chatroom_id}'
        """

        cur.execute(sql)

        column_name = [desc[0] for desc in cur.description] # get column name
        rows = pd.DataFrame(cur.fetchall(), columns=column_name)

        rows.rename(columns=column_mapping, inplace=True)

        return rows


    # Function to calculate trust matrix based on common rated items and ratings distance
    def calculate_trust(self, Group):
        """
        Calculate the trust matrix based on common rated items between group members and their ratings distance.

        Parameters:
            Group (DataFrame): A DataFrame containing group members' ratings for items.

        Returns:
            Trust_matrix (DataFrame): The trust matrix representing the trust levels between members.
        """
        members = Group.index
        no_member = len(members)
    
        Trust_matrix = pd.DataFrame(0.0, index=members, columns=members)
    
        for u in members:
            rated_list_u = Group.loc[u].index[Group.loc[u] > 0]
            count_rated_u = len(rated_list_u)
            ratings_u = Group.loc[u][:]
        
            for v in members:
                if u == v:
                    continue
            
                rated_list_v = Group.loc[v].index[Group.loc[v] > 0]
                count_rated_v = len(rated_list_v)
                ratings_v = Group.loc[v][:]
            
                # 找到 u, v 評分項目的交集
                intersection_uv = set(rated_list_u).intersection(rated_list_v)
                count_intersection = len(intersection_uv)
            
                partnership_uv = count_intersection / count_rated_u
            
                dst_uv = 1 / (1 + distance.euclidean(ratings_u, ratings_v))
            
                trust_uv = (2 * partnership_uv * dst_uv) / (partnership_uv + dst_uv)

                Trust_matrix.at[u, v] = trust_uv
            
        return Trust_matrix

    # Function to calculate Pearson correlation coefficient similarity between members
    def calculate_similarity(self, Group):
        """
        Calculate the Pearson correlation coefficient (PCC) similarity matrix between group members.

        Parameters:
            Group (DataFrame): A DataFrame containing group members' ratings for items.

        Returns:
            PCC_df (DataFrame): The PCC similarity between group members.
        """
        members = Group.index
        ratings = Group.to_numpy()  # Convert DataFrame to a NumPy array

        # Calculate the Pearson correlation coefficient similarity
        PCC = np.corrcoef(ratings, rowvar=True)
    
        # Convert the matrix to a DataFrame with proper index and columns
        PCC_df = pd.DataFrame(PCC, index=members, columns=members)

        return PCC_df

    # Function to identify leader within a group based on Trust and Similarity matrices
    def identify_leader(self, Trust_matrix, Similarity_matrix, total_members):
        """
        Identify the leader within a group based on Trust and Similarity matrices.

        Parameters:
            Trust_matrix (DataFrame): The trust matrix representing the trust levels between members.
            Similarity_matrix (DataFrame): The PCC similarity matrix between group members.
            total_members (int): Total number of members in the group.

        Returns:
            leader_id (string): ID of the identified leader.
            leader_impact (float): Impact value of the identified leader on group preferences.
            sorted_ids (list)
        """
        trust_sum = np.sum(Trust_matrix.values, axis=0) - 1
        similarity_sum = np.sum(Similarity_matrix.values, axis=0) - 1
        ts_sumation = trust_sum + similarity_sum

        # sort ts_sumation in descending order
        sorted_ts_sumation = np.argsort(ts_sumation)[::-1]  # [::-1] makes it descending

        # Get the sorted user IDs and their corresponding impact scores
        sorted_ids = Trust_matrix.index[sorted_ts_sumation]

        # The ID with the highest score (leader)
        LeaderId = np.argmax(ts_sumation)

        LeaderImpact = ts_sumation[LeaderId] / (total_members - 1)

        return Trust_matrix.index[LeaderId], LeaderImpact, sorted_ids.tolist()


    # Function to calculate influence weight based on leader's impact, similarity, and trust
    def calculate_influence_weight(self, leader_id, leader_impact, similarity_uv, trust_uv, v):
        """
        Calculate the influence weight based on leader's impact, similarity, and trust.

        Parameters:
            leader_id (int): ID of the identified leader.
            leader_impact (float): Impact value of the identified leader on group preferences.
            similarity_uv (float): Similarity score between two members.
            trust_uv (float): Trust score between two members.
            v (int): ID of the member being considered.

        Returns:
            weight_uv (float): Calculated influence weight.
        """
        if v == leader_id:
            weight_uv = (1/2) * ((leader_impact + (similarity_uv * trust_uv)) / (similarity_uv + trust_uv))
        else:
            weight_uv = (similarity_uv * trust_uv) / (similarity_uv + trust_uv)
        
        return weight_uv

    # Function to calculate influenced ratings for group members and items
    def influenced_rating(self, group):
        """
        Calculate influenced ratings for group members and items.

        Parameters:
            group (DataFrame): A DataFrame containing group members' ratings for items.

        Returns:
            influenced_ratings (DataFrame): DataFrame containing influenced ratings for group members and items.
            leader_id (string)
            sorted_ids (list)
        """
        members = group.index
        items = group.columns
        num_members, num_items = len(members), len(items)

        # Calculate trust and similarity matrices
        trust_matrix = self.calculate_trust(group)
        similarity_matrix = self.calculate_similarity(group)

        # Identify the leader and their impact
        leader_id, leader_impact, sorted_ids = self.identify_leader(trust_matrix, similarity_matrix, num_members)

        influenced_ratings = pd.DataFrame(0.0, index=members, columns=items)

        for u in members:
            for i in items:
                score_ui = group.at[u, i]
                influence = 0

                if score_ui > 0:
                    for v in members:
                        if v != u:
                            score_vi = group.at[v, i]
                            similarity_uv = similarity_matrix.at[u, v]
                            trust_uv = trust_matrix.at[u, v]
                            weight_vu = self.calculate_influence_weight(leader_id, leader_impact, similarity_uv, trust_uv, v)

                            if score_vi > 0:
                                influence += weight_vu * (score_vi - score_ui)

                    influenced_ratings.at[u, i] = score_ui + influence

            return leader_id, influenced_ratings, sorted_ids

    # Function to calculate opinion weight through sorted_ids
    def calculate_opinion_weight(self, sorted_ids):
        total_users = len(sorted_ids)

        # original weight
        raw_weights = [(total_users - index) for index in range(total_users)]
        total_raw_weight = sum(raw_weights)
    
        opinion_weight = []

        for index, user_id in enumerate(sorted_ids):
            weight = raw_weights[index] / total_raw_weight
            weight_2f = float(f"{weight:.2f}")
            opinion_weight.append({"userId": user_id, "weight": weight_2f})
    
        return opinion_weight

    def main(self, chatroom_id):
        group = self.get_user_preference(chatroom_id)

        # remove 'user_id' 列，並將其設為 index
        group.set_index('user_id', inplace=True)

        leader_id, influenced_ratings, sorted_ids = self.influenced_rating(group)

        # aggregate group's ratings
        avg_influenced_ratings = influenced_ratings.mean()
        top_3_preferences = avg_influenced_ratings.nlargest(3).index.to_list()

        # get member's opinion weight
        opinion_weight = self.calculate_opinion_weight(sorted_ids)

        return leader_id, top_3_preferences, opinion_weight