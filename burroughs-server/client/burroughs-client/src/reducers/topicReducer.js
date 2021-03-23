import {
    SET_TOPICS,
    CLOSE_DESCRIPTION,
    SET_SCHEMA,
    TOPIC_DELETING,
    TOPIC_DELETED,
    DELETE_ERROR,
    CLEANING_UP,
    CLEANED_UP
} from '../actions/actionTypes';

const initialState = {
    topics: [],
    topicSchema: null,
    topicDeleting: false,    
    topicDeleteError: null,
    cleaningUp: false
};

const topicReducer = (state = initialState, action) => {
    if (action.type === SET_TOPICS) {
        return {
            ...state,
            topics: action.payload
        };
    }
    else if (action.type === SET_SCHEMA) {        
        return {
            ...state,
            topicSchema: action.payload,
            topicDeleteError: null
        };
    }
    else if (action.type === CLOSE_DESCRIPTION) {
        return {
            ...state,
            topicSchema: null
        };
    }    
    else if (action.type === TOPIC_DELETING) {
        return {
            ...state,
            topicDeleting: true
        };
    }
    else if (action.type === TOPIC_DELETED) {
        return {
            ...state,
            topicDeleting: false,
            topicSchema: null
        };
    }
    else if (action.type === DELETE_ERROR) {
        return {
            ...state,
            topicDeleting: false,
            topicDeleteError: action.payload
        };
    }
    else if (action.type === CLEANING_UP) {
        return {
            ...state,
            cleaningUp: true
        };
    }
    else if (action.type === CLEANED_UP) {
        return {
            ...state,
            cleaningUp: false
        };
    }
    return state;
};


export default topicReducer;