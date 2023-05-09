def generate_record_id(racer_id, date, time):
    return hash(f'{racer_id}::{date}::{time}')