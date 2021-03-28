export const INCREASE_COL = "INCREASE_COL";
export const INCREASE_ROW = "INCREASE_ROW";


export const increaseCol = amount => {
    return {
        type: INCREASE_COL,
        payload: amount
    };
};

export const increaseRow = amount => {
    return {
        type: INCREASE_ROW,
        payload: amount
    };
};