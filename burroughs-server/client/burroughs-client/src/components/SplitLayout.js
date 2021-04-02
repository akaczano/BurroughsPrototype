import React from 'react';

import CodeEditor from './CodeEditor';
import TopicList from './TopicList';
import DatabaseInfo from './DatabaseInfo';
import OutputTabs from './OutputTabs';
import Gutter from './Gutter';
import ToolBar from './ToolBar';

const colStyle = {
    margin: '0px',
    padding: '0px',    
    overflow: 'hidden'
}

class SplitLayout extends React.Component {
    state = {
        columnSizes: [25, 75],
        rowSize: 37
    };

    columnChanged = newWidth => {                
        this.setState({ columnSizes: [newWidth, 100 - newWidth] })
        window.dispatchEvent(new Event('resize'));
    }
    rowChanged = newHeight => {        
        this.setState({rowSize: newHeight});
        window.dispatchEvent(new Event('resize'));
    }

    render() {
        return (
            <>
                <div style={{ ...colStyle, width: this.state.columnSizes[0] + '%' }}>
                    <TopicList />
                    <DatabaseInfo />
                </div>
                <Gutter 
                    style={{ ...colStyle, width: '0.5%' }} 
                    onAdjust={this.columnChanged}
                    currentSize={this.state.columnSizes[0]}
                    min="15"
                    max="40"
                    mode="vertical"
                />
                <div style={{ ...colStyle, width: this.state.columnSizes[1] - 0.5 + '%' }}>
                    <ToolBar />
                    <CodeEditor style={{ margin: '0px', height: this.state.rowSize + 'vh' }} />
                    <Gutter 
                        onAdjust={this.rowChanged}
                        currentSize={this.state.rowSize}
                        min="15"
                        max="70"
                        mode="horizontal"
                    />
                    <OutputTabs style={{ margin: '0px' }} rowSize={this.state.rowSize} />
                </div>
            </>
        );
    }

}

export default SplitLayout;