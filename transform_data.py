import pandas as pd
import json
from typing import List, Dict

def split_long_paragraph(paragraph: str, max_words: int = 200) -> List[str]:
    """
    Разбивает длинный параграф на части по max_words слов
    """
    words = paragraph.split()
    if len(words) <= max_words:
        return [paragraph]
    
    parts = []
    for i in range(0, len(words), max_words):
        part = ' '.join(words[i:i + max_words])
        parts.append(part)
    
    return parts

def split_text_into_chunks(text: str, title: str, source: str, min_words: int = 50, max_words: int = 200, start_chunk_id: int = 0) -> List[Dict]:
    """
    Разбивает текст на чанки с учетом символов \n как границ смысловых блоков
    """
    chunks = []
    
    # Сначала создаем чанк из заголовка
    if title and '.pdf' not in title and title.strip():
        title_chunk = {
            "text": title,
            "title": title,
            "source": source,
            "chunk_id": start_chunk_id,
            "word_count": len(title.split())
        }
        chunks.append(title_chunk)
        start_chunk_id += 1
    
    # Разделяем текст на смысловые блоки по \n
    paragraphs = text.split('\n')
    
    current_chunk = []
    current_word_count = 0
    chunk_id = start_chunk_id
    
    for paragraph in paragraphs:
        # Убираем лишние пробелы и проверяем, что параграф не пустой
        clean_paragraph = paragraph.strip()
        if not clean_paragraph:
            continue
        
        # Если параграф слишком длинный, разбиваем его на части
        words_in_paragraph = len(clean_paragraph.split())
        
        if words_in_paragraph > max_words:
            # Разбиваем длинный параграф на части
            paragraph_parts = split_long_paragraph(clean_paragraph, max_words)
            
            for part in paragraph_parts:
                part_word_count = len(part.split())
                
                # Если добавление части превысит максимум, сохраняем текущий чанк
                if current_word_count + part_word_count > max_words and current_word_count >= min_words:
                    chunk_text = ' '.join(current_chunk)
                    chunks.append({
                        "text": chunk_text,
                        "title": title,
                        "source": source,
                        "chunk_id": chunk_id,
                        "word_count": current_word_count
                    })
                    chunk_id += 1
                    current_chunk = [part]
                    current_word_count = part_word_count
                else:
                    current_chunk.append(part)
                    current_word_count += part_word_count
        else:
            # Обычный параграф (не слишком длинный)
            # Если текущий чанк пустой, просто добавляем параграф
            if current_word_count == 0:
                current_chunk.append(clean_paragraph)
                current_word_count += words_in_paragraph
                continue
                
            # Проверяем, не превысит ли добавление параграфа максимальный размер
            if current_word_count + words_in_paragraph > max_words:
                # Если текущий чанк уже достиг минимума, сохраняем его
                if current_word_count >= min_words:
                    chunk_text = ' '.join(current_chunk)
                    chunks.append({
                        "text": chunk_text,
                        "title": title,
                        "source": source,
                        "chunk_id": chunk_id,
                        "word_count": current_word_count
                    })
                    chunk_id += 1
                    current_chunk = [clean_paragraph]
                    current_word_count = words_in_paragraph
                else:
                    # Если текущий чанк меньше минимума, но добавление параграфа превысит максимум,
                    # все равно добавляем и сохраняем
                    current_chunk.append(clean_paragraph)
                    current_word_count += words_in_paragraph
                    chunk_text = ' '.join(current_chunk)
                    chunks.append({
                        "text": chunk_text,
                        "title": title,
                        "source": source,
                        "chunk_id": chunk_id,
                        "word_count": current_word_count
                    })
                    chunk_id += 1
                    current_chunk = []
                    current_word_count = 0
            else:
                # Добавляем параграф к текущему чанку
                current_chunk.append(clean_paragraph)
                current_word_count += words_in_paragraph
    
    # Добавляем последний чанк, если он не пустой
    if current_chunk and current_word_count > 0:
        chunk_text = ' '.join(current_chunk)
        # Сохраняем чанк если он достиг минимума или это единственный контент-чанк
        if current_word_count >= min_words or (len(chunks) == 1 and chunks[0]['text'] == title):
            chunks.append({
                "text": chunk_text,
                "title": title,
                "source": source,
                "chunk_id": chunk_id,
                "word_count": current_word_count
            })
    
    return chunks

def process_dataframe_to_chunks(df: pd.DataFrame, min_words: int = 50, max_words: int = 200) -> List[Dict]:
    """
    Обрабатывает DataFrame и создает чанки для всех строк
    """
    all_chunks = []
    global_chunk_id = 0
    
    for index, row in df.iterrows():
        text = str(row['text']) if pd.notna(row['text']) else ""
        title = str(row['title']) if pd.notna(row['title']) else ""
        source = str(row['url']) if pd.notna(row['url']) else ""
        
        if text and text.strip() or title and title.strip():
            chunks = split_text_into_chunks(text, title, source, min_words, max_words, start_chunk_id=global_chunk_id)
            
            # Обновляем global_chunk_id для следующего документа
            if chunks:
                global_chunk_id = chunks[-1]['chunk_id'] + 1
            
            all_chunks.extend(chunks)
    
    return all_chunks

def main():
    # Загружаем данные из CSV
    try:
        df = pd.read_csv('input_data/websites.csv')
        print(f"Загружено строк: {len(df)}")
        
        # Проверяем наличие необходимых колонок
        required_columns = ['web_id', 'url', 'kind', 'title', 'text']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"Ошибка: отсутствуют колонки: {missing_columns}")
            return
        
        # Обрабатываем данные
        chunks = process_dataframe_to_chunks(df, min_words=50, max_words=200)
        
        # Проверяем чанки на превышение максимального размера
        oversized_chunks = [chunk for chunk in chunks if chunk['word_count'] > 200]
        if oversized_chunks:
            print(f"ВНИМАНИЕ: Найдено {len(oversized_chunks)} чанков с превышением максимального размера:")
            for chunk in oversized_chunks[:5]:  # Показываем первые 5
                print(f"  Чанк {chunk['chunk_id']}: {chunk['word_count']} слов")
        
        # Сохраняем в JSON
        output_file = 'output_chunks.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(chunks, f, ensure_ascii=False, indent=2)
        
        print(f"Создано чанков: {len(chunks)}")
        print(f"Результат сохранен в: {output_file}")
        
        # Статистика по чанкам
        if chunks:
            word_counts = [chunk['word_count'] for chunk in chunks]
            print(f"Слов в чанках: мин {min(word_counts)}, макс {max(word_counts)}, сред {sum(word_counts) / len(word_counts):.1f}")
            
            # Показываем распределение по размерам
            size_ranges = {
                "1-50": len([w for w in word_counts if w <= 50]),
                "51-100": len([w for w in word_counts if 51 <= w <= 100]),
                "101-150": len([w for w in word_counts if 101 <= w <= 150]),
                "151-200": len([w for w in word_counts if 151 <= w <= 200]),
                "200+": len([w for w in word_counts if w > 200])
            }
            print(f"Распределение чанков по размеру: {size_ranges}")
        
    except FileNotFoundError:
        print("Ошибка: Файл 'input_data/websites.csv' не найден")
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    main()