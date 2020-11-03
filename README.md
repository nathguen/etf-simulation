# etf-simulation

## Project

The goal of this project is to produce ETF simulations that reflect an automated, thought-free approach to ETF investing with high-yield potential. 


## Project Philosophies

### Automation

Bringing emotion (i.e., FOMO -- Fear Of Missing Out) into investing is a bad strategy, as it poses very high risk with the potential for little, if any, reward. Therefore, to mitigate emotional investing, we want to use rule-based investment strategem instead; this lends itself well to automation, as computers are very good at following rules without allowing emotions to get in the way.

### Probability

While we cannot know, at the time of investment, which ETFs will perform the best, we CAN determine the probability of how well past performance indicates future performance.

There is a balancing act with any investment strategy. 
* Keeping money in cash (i.e., bank account) yields some of the least risk, but it also has little reward. (0.85% / yr)
* Investing in indexed funds produces a fairly consistent reward, with some degree of risk. (7% / 10yr)
* Investing in stocks directly can produce a high reward, but it's very risky because the company may go under in the future. (varied%)

This project seeks to understand and quantity the risks, rewards, and probabilities of using ETFs, and if it can be turned into a viable automated investment strategy.

### Project Assumptions

#### Data Source

This project currently uses Yahoo Finance's APIs to fetch the entire ETF histories of every known ETF in the North American market. It would be great to include more continental markets going forward to determine if the patterns remain constant across all markets.

#### Budget

While budgets change over time, it's significantly easier to model the data when the budget remains constant. The default amount is $4000/month, but this can be easily updated in the `src/config.py` file.

#### Inflation

This project does not currently adjust for inflation, but certainly should in future iterations.


## Set Up

### Install Condas

This project will rely pretty heavily on jupyter notebooks, and the module structures associated with running jupyter notebook files. Therefore, it is recommended to install anaconda and utilize python v3 when working with this project.

### Run the Project

1. Open `main.py` at the root of the project
2. Set the interpreter to be conda v3
3. Run `main.py`
   1. Note: there is a known issue with running it the first time. Run it a second time to get it over that hump.


Each time the project runs, it will randomly set the configs in `config.py` within the parameter boundaries that are set there. The simulation will run `n` number of times, based on the `runs` value in the `run_configs` function in `main.py`. Keep in mind that each `run` is fairly slow (~5-28 seconds a piece), so running many simulations will take a while.

### Files Produced

Each project run will produce:

1. `summaries.csv` that holds all of the high-level summary data for the summation of the simulations. If two simulations ran, the `summaries.csv` will have two rows of results sets; if eight simulations ran, the `summaries.csv` will have eight rows of results sets, etc.
   1. Each Row of `summaries.csv` will contain an ID which can be mapped back to the filenames in `portfolios/`
2.  `portfolios/*` that holds all of the raw data results for the simulations that ran in an `.xlsx` format. There are currently two sheets within each `.xlsx` file:
    1.  Summary (high level information also found in `summaries.csv`)
    2.  Data (holds all of the raw data results)


## Notes

This project is still in its infancy; therefore, much of the project's hierarchy and functionality is still in flux.
