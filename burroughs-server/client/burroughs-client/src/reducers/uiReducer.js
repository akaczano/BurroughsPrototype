import { INCREASE_COL, INCREASE_ROW } from '../actions/uiActions'

const initialState = {
    columnSizes: [25, 75],
    rowSize: 37    
};

const uiReducer = (state=initialState, action ) => {
    if (action.type === INCREASE_COL) {
        return {
            ...state,
            columnSizes: [state.columnSizes[0] + action.payload, state.columnSizes[1] - action.payload]
        };
    }
    else if (action.type === INCREASE_ROW) {
        return {
            ...state,
            rowSize: state.rowSize + action.payload
        };
    }
    return state;
}

export default uiReducer;