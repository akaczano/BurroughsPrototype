import {
    SET_DATA,
    DATA_LOADING,
    DATA_ERROR,
    SET_QUERY_TEXT
} from '../actions/actionTypes';

const initialState = {
    data: null,
    dataLoading: false,
    dataError: null,
    queryText: ''    
};

const dataReducer = (state=initialState, action) => {
    if (action.type === SET_DATA) {
        return {
            ...state,
            data: action.payload,
            dataLoading: false,
            dataError: null
        };
    }
    else if (action.type === DATA_LOADING) {
        return {
            ...state,
            dataLoading: true
        };
    }
    else if (action.type === DATA_ERROR) {        
        return {
            ...state,
            dataLoading: false,
            dataError: action.payload
        };
    }  
    else if (action.type === SET_QUERY_TEXT) {
        return {
            ...state,
            queryText: action.payload
        };
    }
    return state;
};
export default dataReducer;