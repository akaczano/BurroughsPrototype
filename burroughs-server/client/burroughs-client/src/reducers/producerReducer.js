import {
    PRODUCERS_LOADED
} from '../actions/actionTypes';

const initialState = {
    producers: [],    
};

const producerReducer = (state = initialState, action) => {
    if (action.type === PRODUCERS_LOADED) {
        return {
            ...state,
            producers: action.payload
        };
    }
    return state;
};
export default producerReducer;