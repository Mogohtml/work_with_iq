from itertools import combinations


    """Генерация всех уникальных комбинаций из 2–4 ключевых слов."""
    all_combinations = set()
    for r in range(min_words, max_words + 1):
        for combo in combinations(keywords, r):
            all_combinations.add(" ".join(combo))
    return list(all_combinations)