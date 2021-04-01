import React from 'react';
import { connect } from 'react-redux';

import { setCode } from '../actions/basicActions';
import AceEditor from 'react-ace';

import "ace-builds/src-noconflict/mode-mysql";
import "ace-builds/src-noconflict/theme-xcode";


class CodeEditor extends React.Component {


    render() {
        return (
            <AceEditor 
                mode="mysql"
                theme="xcode"
                onChange={code => this.props.setCode(code)}
                value={this.props.code}                
                style={{ ...this.props.style, width: '100%' }}
                fontSize="16px"       
                editorProps={{ $blockScrolling: true }}                         
            />
        );
    }
}

const mapStateToProps = state => {
    return {
        code: state.core.code        
    };
};

export default connect(mapStateToProps, { setCode })(CodeEditor);