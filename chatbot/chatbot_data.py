"""
Applies the chatbot's requested demand changes to the demand data and
draws a before/after comparison chart.
"""
import pandas as pd
import numpy as np
import os
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# demand.csv covers 2024-2025. We match dates against 2024 data but show the
# current year on the plot, so the demo works whatever year the chatbot picks.
DATA_START_YEAR = 2024
DATA_END_YEAR = 2025
DISPLAY_YEAR = datetime.now().year


def _mask_shift_years(start_date):
    """How many years to add to a date so it lands in DATA_START_YEAR."""
    return DATA_START_YEAR - start_date.year


def _shift_years(dt, years):
    return dt if years == 0 else (dt + pd.DateOffset(years=years))


def apply_lifestyle_update(json_payload, input_csv="data/demand.csv", output_csv="data/chatbot/updated_demand.csv"):
    """Applies the chatbot's changes to the demand data and saves the result as a CSV."""
    print("\nApplying coefficients...")

    try:
        df = pd.read_csv(input_csv)
    except FileNotFoundError:
        print(f"Error: Could not find {input_csv}.")
        return None

    # dayfirst=True so DD/MM/YYYY dates are read correctly
    df['timestamp'] = pd.to_datetime(df['timestamp'], dayfirst=True)

    # Columns for the before/after values and which rows were changed
    df['pre_demand'] = df['User']
    df['post_demand'] = df['User'].copy()
    df['multiplier_applied'] = np.nan
    df['is_masked'] = False

    # Read the change details out of the chatbot's JSON
    mod_type = json_payload['modification']['type']
    mod_value = float(json_payload['modification']['value'])
    
    timing = json_payload['timing']
    days_of_week = timing['days_of_week']
    start_hour = int(timing['start_hour'])
    end_hour = int(timing['end_hour'])
    
    start_date = pd.to_datetime(timing['start_date'], dayfirst=True)
    end_date = pd.to_datetime(timing['end_date'], dayfirst=True)

    # Shift the dates into the data's year for matching, and record the shift
    # needed to display the original year on the plot.
    mask_shift = _mask_shift_years(start_date)
    if mask_shift != 0:
        print(f"  Shifting dates by {mask_shift:+d}y so start lands in {DATA_START_YEAR}")
        start_date = _shift_years(start_date, mask_shift)
        end_date = _shift_years(end_date, mask_shift)
    json_payload['year_shift'] = DISPLAY_YEAR - DATA_START_YEAR

    # Push the end date to 23:59 so the whole final day is included
    if end_date.hour == 0 and end_date.minute == 0:
         end_date = end_date.replace(hour=23, minute=59)

    # Filters for the date range, the chosen weekdays, and the hour range
    date_mask = (df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)
    day_mask = df['timestamp'].dt.dayofweek.isin(days_of_week)
    hour_mask = (df['timestamp'].dt.hour >= start_hour) & (df['timestamp'].dt.hour <= end_hour)

    # Rows that match all three filters are the ones we change
    final_mask = date_mask & day_mask & hour_mask
    df.loc[final_mask, 'is_masked'] = True

    if mod_type == 'scale':
        df.loc[final_mask, 'post_demand'] = df.loc[final_mask, 'pre_demand'] * mod_value
        df.loc[final_mask, 'multiplier_applied'] = mod_value

        # Keep demand from going above 1.0
        df['post_demand'] = df['post_demand'].clip(upper=1.0)

    elif mod_type == 'fixed':
        df.loc[final_mask, 'post_demand'] = mod_value
        df.loc[final_mask, 'multiplier_applied'] = mod_value

    output_df = df[['timestamp', 'pre_demand', 'post_demand', 'multiplier_applied', 'is_masked']]

    os.makedirs(os.path.dirname(output_csv), exist_ok=True)
    output_df.to_csv(output_csv, index=False)
    print(f"Success! Modified data saved to: {output_csv}")
    
    return output_df

def get_plot_window(df, json_payload):
    """Picks the slice of data to show on the chart."""
    if not df['is_masked'].any():
        return df.head(100)

    category = json_payload.get('category', '')
    first_masked_time = df[df['is_masked']]['timestamp'].iloc[0]

    if category == 'Vacation':
        # Apply the same date shift as apply_lifestyle_update so the slice
        # lands on the matching CSV rows.
        raw_start = pd.to_datetime(json_payload['timing']['start_date'], dayfirst=True)
        raw_end = pd.to_datetime(json_payload['timing']['end_date'], dayfirst=True)
        shift = _mask_shift_years(raw_start)
        start_date = _shift_years(raw_start, shift)
        end_date = _shift_years(raw_end, shift)

        plot_start = start_date.normalize() - pd.Timedelta(days=1)
        plot_end = end_date.normalize() + pd.Timedelta(days=2)
        
    else:
        # EV and Worker show the Mon-Fri week containing the first changed row
        data_monday = first_masked_time.normalize() - pd.Timedelta(days=first_masked_time.dayofweek)
        plot_start = data_monday
        plot_end = data_monday + pd.Timedelta(days=4, hours=23)  # Friday 23:00

    plot_df = df[(df['timestamp'] >= plot_start) & (df['timestamp'] <= plot_end)].copy()
    return plot_df


def plot_demand_comparison(plot_df, category, year_shift=0):
    """Draws a line chart comparing the original and updated demand."""
    print("Generating comparison graph...")

    plot_df = plot_df.copy()

    if category == 'Vacation':
        # Move the dates forward so the chart shows the requested year
        if year_shift > 0:
            plot_df['timestamp'] = plot_df['timestamp'] + pd.DateOffset(years=year_shift)
    else:
        # Worker / EV: move the data's week onto this week so the x-axis reads
        # as the current week.
        data_monday = plot_df['timestamp'].iloc[0].normalize()
        data_monday = data_monday - pd.Timedelta(days=data_monday.dayofweek)
        today = pd.Timestamp.now().normalize()
        next_monday = today + pd.Timedelta(days=(7 - today.dayofweek) % 7 or 7)
        plot_df['timestamp'] = plot_df['timestamp'] + (next_monday - data_monday)

    plt.figure(figsize=(10, 5))

    # Updated demand line, drawn across the whole window
    plt.plot(plot_df['timestamp'], plot_df['post_demand'],
             label='Updated Demand', color='#0072B2', linewidth=2)

    # Include the rows either side of a changed row so the lines join up
    visual_mask = (
        plot_df['is_masked'] |
        plot_df['is_masked'].shift(1).fillna(False) |
        plot_df['is_masked'].shift(-1).fillna(False)
    )

    # Only show the original demand where the change happened
    original_masked = plot_df['pre_demand'].where(visual_mask, np.nan)

    plt.plot(plot_df['timestamp'], original_masked,
             label='Original Demand', color="#9E9E9E", linewidth=2, linestyle='--')

    # Shade the gap between the two lines
    plt.fill_between(plot_df['timestamp'],
                     plot_df['pre_demand'],
                     plot_df['post_demand'],
                     where=visual_mask,
                     interpolate=True,
                     color='#E69F00', alpha=0.3, label='Net Change')

    plt.title(f"User Electricity Consumption: {category} Profile", fontsize=14)

    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y'))
    plt.xlabel("Date (DD/MM/YYYY)", fontsize=12)
    plt.xticks(rotation=30)
    plt.ylabel("User demand (kWh, normalised)", fontsize=12)
    plt.ylim(0, 1.05)
    plt.grid(True, alpha=0.3)
    plt.legend(loc='upper right')
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    # Running this file directly tests the logic with a sample payload
    mock_payload = {
        "mission_check": "Scale demand for Working from Home?",
        "category": "Worker",
        "modification": {"type": "scale", "value": 1.2},
        "timing": {
            "repeat": "weekly",
            "days_of_week": [4], # Friday
            "start_date": "01/01/2024",
            "end_date": "31/12/2025",
            "start_hour": 9,
            "end_hour": 17
        }
    }
    
    # Needs a 'data/demand.csv' file to run
    test_run = apply_lifestyle_update(mock_payload)
    modified_df = apply_lifestyle_update(json_payload=mock_payload)

    if modified_df is not None:
        plot_data = get_plot_window(modified_df, mock_payload)
        plot_demand_comparison(plot_data, mock_payload['category'])
    
    if test_run is not None:
        print("\nPreview of modified rows:")
        print(test_run[test_run['is_masked'] == True].head())