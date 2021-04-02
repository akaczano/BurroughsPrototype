import {
    SET_CONNECTION,   
    SET_CODE,
    SET_OUTPUT_TABLE,    
    SET_DATABASE,    
    APPEND_MESSAGE
} from '../actions/actionTypes';

const initialState = {
    connection: null,
    code: '',      
    table: null,  
    dbInfo: {
        hostname: '',
        username: '',
        password: '',
        database: '',
        table: ''
    },           
    consoleMessages: [],
    lastConsoleRequest: Date.now(), 
};

const reducer = (state = initialState, action) => {
    if (action.type === SET_CONNECTION) {
        return {
            ...state,
            connection: action.payload
        }
    }    

    else if (action.type === SET_DATABASE) {        
        return {
            ...state,
            dbInfo: {
                ...state.dbInfo,
                database: action.payload.database,
                hostname: action.payload.hostname,
                username: action.payload.username,
                table: action.payload.table,
                data: null
            }
        };
    }
    else if (action.type === SET_CODE) {
        return {
            ...state,
            code: action.payload
        };
    }
    else if (action.type === SET_OUTPUT_TABLE) {
        return {
            ...state,
            dbInfo: {
                ...state.dbInfo,
                table: action.payload
            }
        };
    }
    else if (action.type === APPEND_MESSAGE) {
        let newMessages = state.consoleMessages.slice();
        for (const m of action.payload) {
            if (!newMessages.find(mes => mes.time === m.time) && m.text.length > 0) {
                newMessages.push(m);
            }
        }
        return {
            ...state,
            consoleMessages: newMessages,
            lastConsoleRequest: Date.now()
        };
    }  
    
    return state;
};

export default reducer;